/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.raid;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockMissingException;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

/**
 * Represents a generic decoder that can be used to read a file with
 * corrupt blocks by using the parity file.
 */
public class Decoder {
  public static final Log LOG = LogFactory.getLog(
                                  "org.apache.hadoop.raid.Decoder");
  public static final int DEFAULT_PARALLELISM = 4;
  protected Configuration conf;
  protected int parallelism;
  protected Codec codec;
  protected ErasureCode code;
  protected Random rand;
  protected int bufSize;
  protected byte[][] readBufs;
  protected byte[][] writeBufs;

  public Decoder(Configuration conf, Codec codec) {
    this.conf = conf;
    this.parallelism = conf.getInt("raid.encoder.parallelism",
                                   DEFAULT_PARALLELISM);
    this.codec = codec;
    this.code = codec.createErasureCode(conf);
    this.rand = new Random();
    this.bufSize = conf.getInt("raid.decoder.bufsize", 1024 * 1024);
    this.writeBufs = new byte[codec.parityLength][];
    this.readBufs = new byte[codec.parityLength + codec.stripeLength][];
    allocateBuffers();
  }

  private void allocateBuffers() {
    for (int i = 0; i < codec.parityLength; i++) {
      writeBufs[i] = new byte[bufSize];
    }
  }

  private void configureBuffers(long blockSize) {
    if ((long)bufSize > blockSize) {
      bufSize = (int)blockSize;
      allocateBuffers();
    } else if (blockSize % bufSize != 0) {
      bufSize = (int)(blockSize / 256L); // heuristic.
      if (bufSize == 0) {
        bufSize = 1024;
      }
      bufSize = Math.min(bufSize, 1024 * 1024);
      allocateBuffers();
    }
  }

  /**
   * Recovers a corrupt block to local file.
   *
   * @param srcFs The filesystem containing the source file.
   * @param srcPath The damaged source file.
   * @param parityPath The filesystem containing the parity file. This could be
   *        different from fs in case the parity file is part of a HAR archive.
   * @param parityFile The parity file.
   * @param blockSize The block size of the file.
   * @param blockOffset Known location of error in the source file. There could
   *        be additional errors in the source file that are discovered during
   *        the decode process.
   * @param localBlockFile The file to write the block to.
   * @param limit The maximum number of bytes to be written out.
   *              This is to prevent writing beyond the end of the file.
   * @param reporter A mechanism to report progress.
   */
  public void recoverBlockToFile(
    FileSystem srcFs, Path srcPath, FileSystem parityFs, Path parityPath,
    long blockSize, long blockOffset, File localBlockFile, long limit,
    Progressable reporter)
      throws IOException {
    OutputStream out = new FileOutputStream(localBlockFile);
    fixErasedBlock(srcFs, srcPath, parityFs, parityPath,
                  blockSize, blockOffset, limit, out, reporter);
    out.close();
  }

  /**
   * Having buffers of the right size is extremely important. If the the
   * buffer size is not a divisor of the block size, we may end up reading
   * across block boundaries.
   */
  void fixErasedBlock(
      FileSystem srcFs, Path srcFile, FileSystem parityFs, Path parityFile,
      long blockSize, long errorOffset, long limit,
      OutputStream out, Progressable reporter) throws IOException {
    configureBuffers(blockSize);

    int blockIdx = (int) (errorOffset/blockSize);
    int stripeIdx = blockIdx / codec.stripeLength;
    int blockIdxInStripe = blockIdx % codec.stripeLength;
    int erasedLocationToFix = codec.parityLength + blockIdxInStripe;

    FileStatus srcStat = srcFs.getFileStatus(srcFile);
    FileStatus parityStat = parityFs.getFileStatus(parityFile);

    InputStream[] inputs = new InputStream[codec.stripeLength + codec.parityLength];
    List<Integer> erasedLocations = new ArrayList<Integer>();
    // Start off with one erased location.
    erasedLocations.add(erasedLocationToFix);
    List<Integer> locationsToNotRead = new ArrayList<Integer>();

    int boundedBufferCapacity = 2;
    ParallelStreamReader parallelReader = null;
    LOG.info("Need to write " + limit +
             " bytes for erased location index " + erasedLocationToFix);
    try {
      // Loop while the number of written bytes is less than the max.
      for (int written = 0; written < limit; ) {
        try {
          if (parallelReader == null) {
            long offsetInBlock = written;
            buildInputs(srcFs, srcFile, srcStat,
              parityFs, parityFile, parityStat,
              stripeIdx, offsetInBlock,
              inputs, erasedLocations, locationsToNotRead);
            assert(parallelReader == null);
            parallelReader = new ParallelStreamReader(reporter, inputs, bufSize,
              parallelism, boundedBufferCapacity, blockSize);
            parallelReader.start();
          }
          ParallelStreamReader.ReadResult readResult = readFromInputs(
            erasedLocations, limit, reporter, parallelReader);
          int[] erased = new int[codec.parityLength];
          for (int i = 0; i < erased.length; i++) {
            erased[i] = locationsToNotRead.get(i);
          }
          code.decodeBulk(readResult.readBufs, writeBufs, erased);

          int toWrite = (int)Math.min((long)bufSize, limit - written);
          for (int i = 0; i < erasedLocations.size(); i++) {
            if (erasedLocations.get(i) == erasedLocationToFix) {
              out.write(writeBufs[i], 0, toWrite);
              written += toWrite;
              break;
            }
          }
        } catch (IOException e) {
          if (e instanceof TooManyErasedLocations) {
            throw e;
          }
          // Re-create inputs from the new erased locations.
          if (parallelReader != null) {
            parallelReader.shutdown();
            parallelReader = null;
          }
          RaidUtils.closeStreams(inputs);
        }
      }
    } finally {
      if (parallelReader != null) {
        parallelReader.shutdown();
      }
      RaidUtils.closeStreams(inputs);
    }
  }

  private InputStream buildOneInput(
    int stripeIndex, int locationIndex, long offsetInBlock,
    FileSystem srcFs, Path srcFile, FileStatus srcStat,
    FileSystem parityFs, Path parityFile, FileStatus parityStat
    ) throws IOException {
    final long blockSize = srcStat.getBlockSize();

    LOG.info("buildOneInput srcfile " + srcFile + " srclen " + srcStat.getLen() + 
      " parityfile " + parityFile + " paritylen " + parityStat.getLen() +
      " stripeindex " + stripeIndex + " locationindex " + locationIndex + " offsetinblock " + offsetInBlock);
    if (locationIndex < codec.parityLength) {
      // Dealing with a parity file here.
      int parityBlockIdx = (codec.parityLength * stripeIndex + locationIndex);
      long offset = blockSize * parityBlockIdx + offsetInBlock;
      assert(offset < parityStat.getLen());
      LOG.info("Opening " + parityFile + ":" + offset +
        " for location " + locationIndex);
      FSDataInputStream s = parityFs.open(
        parityFile, conf.getInt("io.file.buffer.size", 64 * 1024));
      s.seek(offset);
      return s;
    } else {
      // Dealing with a src file here.
      int blockIdxInStripe = locationIndex - codec.parityLength;
      int blockIdx = (codec.stripeLength * stripeIndex + blockIdxInStripe);
      long offset = blockSize * blockIdx + offsetInBlock;
      if (offset >= srcStat.getLen()) {
        LOG.info("Using zeros for " + srcFile + ":" + offset +
          " for location " + locationIndex);
        return new RaidUtils.ZeroInputStream(blockSize * (blockIdx + 1));
      } else {
        LOG.info("Opening " + srcFile + ":" + offset +
          " for location " + locationIndex);
        FSDataInputStream s = srcFs.open(
            srcFile, conf.getInt("io.file.buffer.size", 64 * 1024));
        s.seek(offset);
        return s;
      }
    }
  }

  /**
   * Builds (codec.stripeLength + codec.parityLength) inputs given some erased locations.
   * Outputs:
   *  - the array of input streams @param inputs
   *  - the list of erased locations @param erasedLocations.
   *  - the list of locations that are not read @param locationsToNotRead.
   */
  private void buildInputs(
    FileSystem srcFs, Path srcFile, FileStatus srcStat,
    FileSystem parityFs, Path parityFile, FileStatus parityStat,
    int stripeIdx, long offsetInBlock,
    InputStream[] inputs, List<Integer> erasedLocations, List<Integer> locationsToNotRead)
      throws IOException {
    boolean redo = false;
    do {
      locationsToNotRead.clear();
      List<Integer> locationsToRead =
        code.locationsToReadForDecode(erasedLocations);
      for (int i = 0; i < inputs.length; i++) {
        boolean isErased = (erasedLocations.indexOf(i) != -1);
        boolean shouldRead = (locationsToRead.indexOf(i) != -1);
        try {
          InputStream stm = null;
          if (isErased || !shouldRead) {
            if (isErased) {
              LOG.info("Location " + i + " is erased, using zeros");
            } else {
              LOG.info("Location " + i + " need not be read, using zeros");
            }
            locationsToNotRead.add(i);
            stm = new RaidUtils.ZeroInputStream(srcStat.getBlockSize() * (
              (i < codec.parityLength) ?
              stripeIdx * codec.parityLength + i :
              stripeIdx * codec.stripeLength + i - codec.parityLength));
          } else {
            stm = buildOneInput(
              stripeIdx, i, offsetInBlock,
              srcFs, srcFile, srcStat,
              parityFs, parityFile, parityStat);
          }
          inputs[i] = stm;
        } catch (IOException e) {
          if (e instanceof BlockMissingException || e instanceof ChecksumException) {
            erasedLocations.add(i);
            redo = true;
            RaidUtils.closeStreams(inputs);
            break;
          } else {
            throw e;
          }
        }
      }
    } while (redo);
    assert(new HashSet(locationsToNotRead).size() == codec.parityLength);
  }

  ParallelStreamReader.ReadResult readFromInputs(
          List<Integer> erasedLocations,
          long limit,
          Progressable reporter,
          ParallelStreamReader parallelReader) throws IOException {
    ParallelStreamReader.ReadResult readResult;
    try {
      long start = System.currentTimeMillis();
      readResult = parallelReader.getReadResult();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while waiting for read result");
    }

    IOException exceptionToThrow = null;
    // Process io errors, we can tolerate upto codec.parityLength errors.
    for (int i = 0; i < readResult.ioExceptions.length; i++) {
      IOException e = readResult.ioExceptions[i];
      if (e == null) {
        continue;
      }
      if (e instanceof BlockMissingException) {
        LOG.warn("Encountered BlockMissingException in stream " + i);
      } else if (e instanceof ChecksumException) {
        LOG.warn("Encountered ChecksumException in stream " + i);
      } else {
        throw e;
      }
      int newErasedLocation = i;
      erasedLocations.add(newErasedLocation);
      exceptionToThrow = e;
    }
    if (exceptionToThrow != null) {
      throw exceptionToThrow;
    }
    return readResult;
  }

}
