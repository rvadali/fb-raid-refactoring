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
import java.io.OutputStream;
import java.util.ArrayList;
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
   * Wraps around fixErasedBlockImpl in order to configure buffers.
   * Having buffers of the right size is extremely important. If the the
   * buffer size is not a divisor of the block size, we may end up reading
   * across block boundaries.
   */
  void fixErasedBlock(
      FileSystem fs, Path srcFile, FileSystem parityFs, Path parityFile,
      long blockSize, long errorOffset, long limit,
      OutputStream out, Progressable reporter) throws IOException {
    configureBuffers(blockSize);
    FSDataInputStream[] inputs = new FSDataInputStream[codec.stripeLength + codec.parityLength];
    int[] erasedLocations = buildInputs(fs, srcFile, parityFs, parityFile,
                                        errorOffset, inputs);
    int blockIdxInStripe = ((int)(errorOffset/blockSize)) % codec.stripeLength;
    int erasedLocationToFix = codec.parityLength + blockIdxInStripe;

    // Allows network reads to go on while decode is going on.
    int boundedBufferCapacity = 2;
    ExecutorService parallelDecoder = Executors.newFixedThreadPool(parallelism);
    ParallelStreamReader parallelReader = new ParallelStreamReader(
      reporter, inputs, bufSize, parallelism, boundedBufferCapacity, blockSize);
    parallelReader.start();
    try {
      writeFixedBlock(inputs, erasedLocations, erasedLocationToFix,
                      limit, out, reporter, parallelReader);
    } finally {
      // Inputs will be closed by parallelReader.shutdown().
      parallelReader.shutdown();
      parallelDecoder.shutdownNow();
    }

  }

  protected int[] buildInputs(FileSystem fs, Path srcFile,
                              FileSystem parityFs, Path parityFile,
                              long errorOffset, FSDataInputStream[] inputs)
      throws IOException {
    LOG.info("Building inputs to recover block starting at " + errorOffset);
    try {
      FileStatus srcStat = fs.getFileStatus(srcFile);
      long blockSize = srcStat.getBlockSize();
      long blockIdx = (int)(errorOffset / blockSize);
      long stripeIdx = blockIdx / codec.stripeLength;
      LOG.info("FileSize = " + srcStat.getLen() + ", blockSize = " + blockSize +
               ", blockIdx = " + blockIdx + ", stripeIdx = " + stripeIdx);
      ArrayList<Integer> erasedLocations = new ArrayList<Integer>();
      // First open streams to the parity blocks.
      for (int i = 0; i < codec.parityLength; i++) {
        long offset = blockSize * (stripeIdx * codec.parityLength + i);
        FSDataInputStream in = parityFs.open(
          parityFile, conf.getInt("io.file.buffer.size", 64 * 1024));
        in.seek(offset);
        LOG.info("Adding " + parityFile + ":" + offset + " as input " + i);
        inputs[i] = in;
      }
      // Now open streams to the data blocks.
      for (int i = codec.parityLength; i < codec.parityLength + codec.stripeLength; i++) {
        long offset = blockSize * (stripeIdx * codec.stripeLength + i - codec.parityLength);
        if (offset == errorOffset) {
          LOG.info(srcFile + ":" + offset +
              " is known to have error, adding zeros as input " + i);
          inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
              offset + blockSize));
          erasedLocations.add(i);
        } else if (offset > srcStat.getLen()) {
          LOG.info(srcFile + ":" + offset +
                   " is past file size, adding zeros as input " + i);
          inputs[i] = new FSDataInputStream(new RaidUtils.ZeroInputStream(
              offset + blockSize));
        } else {
          FSDataInputStream in = fs.open(
            srcFile, conf.getInt("io.file.buffer.size", 64 * 1024));
          in.seek(offset);
          LOG.info("Adding " + srcFile + ":" + offset + " as input " + i);
          inputs[i] = in;
        }
      }
      if (erasedLocations.size() > codec.parityLength) {
        String msg = "Too many erased locations: " + erasedLocations.size();
        LOG.error(msg);
        throw new IOException(msg);
      }
      int[] locs = new int[erasedLocations.size()];
      for (int i = 0; i < locs.length; i++) {
        locs[i] = erasedLocations.get(i);
      }
      return locs;
    } catch (IOException e) {
      RaidUtils.closeStreams(inputs);
      throw e;
    }
  }

  /**
   * Decode the inputs provided and write to the output.
   * @param inputs array of inputs.
   * @param erasedLocations indexes in the inputs which are known to be erased.
   * @param erasedLocationToFix index in the inputs which needs to be fixed.
   * @param limit maximum number of bytes to be written.
   * @param out the output.
   * @throws IOException
   */
  void writeFixedBlock(
          FSDataInputStream[] inputs,
          int[] erasedLocations,
          int erasedLocationToFix,
          long limit,
          OutputStream out,
          Progressable reporter,
          ParallelStreamReader parallelReader) throws IOException {

    LOG.info("Need to write " + limit +
             " bytes for erased location index " + erasedLocationToFix);
    // Loop while the number of written bytes is less than the max.
    for (int written = 0; written < limit; ) {
      erasedLocations = readFromInputs(
        inputs, erasedLocations, limit, reporter, parallelReader);

      code.decodeBulk(readBufs, writeBufs, erasedLocations);

      int toWrite = (int)Math.min((long)bufSize, limit - written);
      for (int i = 0; i < erasedLocations.length; i++) {
        if (erasedLocations[i] == erasedLocationToFix) {
          out.write(writeBufs[i], 0, toWrite);
          written += toWrite;
          break;
        }
      }
    }
  }

  int[] readFromInputs(
          FSDataInputStream[] inputs,
          int[] erasedLocations,
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

      // Found a new erased location.
      if (erasedLocations.length == codec.parityLength) {
        String msg = "Too many read errors";
        LOG.error(msg);
        throw new IOException(msg);
      }

      // Add this stream to the set of erased locations.
      int[] newErasedLocations = new int[erasedLocations.length + 1];
      for (int j = 0; j < erasedLocations.length; j++) {
        newErasedLocations[j] = erasedLocations[j];
      }
      newErasedLocations[newErasedLocations.length - 1] = i;
      erasedLocations = newErasedLocations;
    }
    readBufs = readResult.readBufs;
    return erasedLocations;
  }

}
