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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;
import java.io.FileWriter;
import java.io.BufferedWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.raid.protocol.PolicyInfo;

/**
 * Check for files that have a replication factor of less than 3
 * And which do not have a parity file created for them
 */
public class MissingParityFiles {

  private Configuration conf;
  private boolean directoryTraversalShuffle;
  private int directoryTraversalThreads;
  private FileSystem fs;
  private static final int REPLICATION_LIMIT = 3;

  public MissingParityFiles(Configuration conf) {
    try {
      this.conf = conf;
      this.directoryTraversalShuffle =
          conf.getBoolean(RaidNode.RAID_DIRECTORYTRAVERSAL_SHUFFLE, true);
      this.directoryTraversalThreads =
          conf.getInt(RaidNode.RAID_DIRECTORYTRAVERSAL_THREADS, 4);
      this.fs = new Path(Path.SEPARATOR).getFileSystem(conf);
    } catch(IOException ex) {
      System.err.println("MissingParityFiles exception: " + ex);
    }
  }
  
  public Set<String> findMissingParityFiles(Path root) throws IOException {
    List<Path> allPaths = Arrays.asList(root);
    Set<String> allMissingParityFiles = new HashSet<String>();
    boolean allowUseStandby = true;
    DirectoryTraversal traversal =
        DirectoryTraversal.fileRetriever(allPaths, fs,
            directoryTraversalThreads, directoryTraversalShuffle,
            allowUseStandby);
    
    FileStatus newFile;
    while ((newFile = traversal.next()) != DirectoryTraversal.FINISH_TOKEN) {
      Path filePath = newFile.getPath();
      if (newFile.getReplication() < MissingParityFiles.REPLICATION_LIMIT) {
        boolean found = false;
        if (isParityFile(filePath)) {
          continue;
        }
        for (Codec c : Codec.getCodecs()) {
          ParityFilePair parityPair =
            ParityFilePair.getParityFile(c, filePath, conf);
          if (parityPair != null) {
            found = true;
          }
        }
        if (!found) {
          System.out.println("File with replication < 3 and no parity file: " + filePath);
          allMissingParityFiles.add(filePath.toUri().getPath());
        }
      }
    }
    return allMissingParityFiles;
  }

  public boolean isParityFile(Path filePath) {
    String pathStr = filePath.toUri().getPath();
    for (Codec c : Codec.getCodecs()) {
      if (pathStr.startsWith(c.parityDirectory)) {
        return true;
      }
    }
    return false;
  }

}
