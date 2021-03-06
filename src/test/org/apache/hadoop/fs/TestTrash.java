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
package org.apache.hadoop.fs;


import junit.framework.TestCase;
import java.io.File;
import java.io.IOException;
import java.io.DataOutputStream;
import java.net.URI;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.LocalFileSystem;

/**
 * This class tests commands from Trash.
 */
public class TestTrash extends TestCase {

  private final static Path TEST_DIR =
    new Path(new File(System.getProperty("test.build.data","/tmp")
          ).toString().replace(' ', '+'), "testTrash");

  protected static Path writeFile(FileSystem fs, Path f) throws IOException {
    DataOutputStream out = fs.create(f);
    out.writeBytes("dhruba: " + f);
    out.close();
    assertTrue(fs.exists(f));
    return f;
  }

  protected static Path mkdir(FileSystem fs, Path p) throws IOException {
    assertTrue(fs.mkdirs(p));
    assertTrue(fs.exists(p));
    assertTrue(fs.getFileStatus(p).isDir());
    return p;
  }

  // check that the specified file is in Trash
  public static void checkTrash(FileSystem fs, Path trashRoot,
      Path path) throws IOException {
    Path p = new Path(trashRoot+"/"+ path.toUri().getPath());
    assertTrue(fs.exists(p));
  }

  // check that the specified file is not in Trash
  public static void checkNotInTrash(FileSystem fs, Path trashRoot, String pathname)
                              throws IOException {
    Path p = new Path(trashRoot+"/"+ new Path(pathname).getName());
    assertTrue(!fs.exists(p));
  }

  protected static void trashShell(final FileSystem fs, final Path base)
      throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.trash.interval", "10"); // 10 minute
    conf.set("fs.default.name", fs.getUri().toString());
    FsShell shell = new FsShell();
    shell.setConf(conf);
    Path trashRoot = null;

    // First create a new directory with mkdirs
    Path myPath = new Path(base, "test/mkdirs");
    mkdir(fs, myPath);

    // Second, create a file in that directory.
    Path myFile = new Path(base, "test/mkdirs/myFile");
    writeFile(fs, myFile);

    // Verify that expunge without Trash directory
    // won't throw Exception
    {
      String[] args = new String[1];
      args[0] = "-expunge";
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
    }

    // Verify that we succeed in removing the file we created.
    // This should go into Trash.
    {
      String[] args = new String[2];
      args[0] = "-rm";
      args[1] = myFile.toString();
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);

      trashRoot = shell.getCurrentTrashDir();
      checkTrash(fs, trashRoot, myFile);
    }

    // Verify that we can recreate the file
    writeFile(fs, myFile);

    // Verify that we succeed in removing the file we re-created
    {
      String[] args = new String[2];
      args[0] = "-rm";
      args[1] = new Path(base, "test/mkdirs/myFile").toString();
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
    }

    // Verify that we can recreate the file
    writeFile(fs, myFile);

    // Verify that we succeed in removing the whole directory
    // along with the file inside it.
    {
      String[] args = new String[2];
      args[0] = "-rmr";
      args[1] = new Path(base, "test/mkdirs").toString();
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
    }

    // recreate directory
    mkdir(fs, myPath);

    // Verify that we succeed in removing the whole directory
    {
      String[] args = new String[2];
      args[0] = "-rmr";
      args[1] = new Path(base, "test/mkdirs").toString();
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
    }

    // Check that we can delete a file from the trash
    {
        Path toErase = new Path(trashRoot, "toErase");
        int retVal = -1;
        writeFile(fs, toErase);
        try {
          retVal = shell.run(new String[] {"-rm", toErase.toString()});
        } catch (Exception e) {
          System.err.println("Exception raised from Trash.run " +
                             e.getLocalizedMessage());
        }
        assertTrue(retVal == 0);
        checkNotInTrash (fs, trashRoot, toErase.toString());
        checkNotInTrash (fs, trashRoot, toErase.toString()+".1");
    }

    // simulate Trash removal
    {
      String[] args = new String[1];
      args[0] = "-expunge";
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
    }

    // verify that after expunging the Trash, it really goes away
    checkNotInTrash(fs, trashRoot, new Path(base, "test/mkdirs/myFile").toString());

    // recreate directory and file
    mkdir(fs, myPath);
    writeFile(fs, myFile);

    // remove file first, then remove directory
    {
      String[] args = new String[2];
      args[0] = "-rm";
      args[1] = myFile.toString();
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
      checkTrash(fs, trashRoot, myFile);

      args = new String[2];
      args[0] = "-rmr";
      args[1] = myPath.toString();
      val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);
      checkTrash(fs, trashRoot, myPath);
    }

    // attempt to remove parent of trash
    {
      String[] args = new String[2];
      args[0] = "-rmr";
      args[1] = trashRoot.getParent().getParent().toString();
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == -1);
      assertTrue(fs.exists(trashRoot));
    }
    
    // Verify skip trash option really works
    
    // recreate directory and file
    mkdir(fs, myPath);
    writeFile(fs, myFile);
    
    // Verify that skip trash option really skips the trash for files (rm)
    {
      String[] args = new String[3];
      args[0] = "-rm";
      args[1] = "-skipTrash";
      args[2] = myFile.toString();
      int val = -1;
      try {
        // Clear out trash
        assertEquals(0, shell.run(new String [] { "-expunge" } ));

        val = shell.run(args);
        
      }catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
            e.getLocalizedMessage());
      }
      
      assertFalse(fs.exists(trashRoot)); // No new Current should be created
      assertFalse(fs.exists(myFile));
      assertTrue(val == 0);
    }
    
    // recreate directory and file
    mkdir(fs, myPath);
    writeFile(fs, myFile);
    
    // Verify that skip trash option really skips the trash for rmr
    {
      String[] args = new String[3];
      args[0] = "-rmr";
      args[1] = "-skipTrash";
      args[2] = myPath.toString();

      int val = -1;
      try {
        // Clear out trash
        assertEquals(0, shell.run(new String [] { "-expunge" } ));
        
        val = shell.run(args);
        
      }catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
            e.getLocalizedMessage());
      }

      assertFalse(fs.exists(trashRoot)); // No new Current should be created
      assertFalse(fs.exists(myPath));
      assertFalse(fs.exists(myFile));
      assertTrue(val == 0);
    }
  }

  public static void trashNonDefaultFS(Configuration conf) throws IOException {
    conf.set("fs.trash.interval", "10"); // 10 minute
    // attempt non-default FileSystem trash
    {
      final FileSystem lfs = FileSystem.getLocal(conf);
      Path p = TEST_DIR;
      Path f = new Path(p, "foo/bar");
      if (lfs.exists(p)) {
        lfs.delete(p, true);
      }
      try {
        f = writeFile(lfs, f);

        FileSystem.closeAll();
        FileSystem localFs = FileSystem.get(URI.create("file:///"), conf);
        Trash lTrash = new Trash(localFs, conf);
        lTrash.moveToTrash(f.getParent());
        checkTrash(localFs, lTrash.getCurrentTrashDir(), f);
      } finally {
        if (lfs.exists(p)) {
          lfs.delete(p, true);
        }
      }
    }
  }

  public void testTrash() throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    trashShell(FileSystem.getLocal(conf), TEST_DIR);
  }

  public void testPluggableTrash() throws IOException {
    Configuration conf = new Configuration();

    // Test plugged TrashPolicy
    conf.setClass("fs.trash.classname", TestTrashPolicy.class, TrashPolicy.class);
    Trash trash = new Trash(conf);
    assertTrue(trash.getTrashPolicy().getClass().equals(TestTrashPolicy.class));
  }


  public void testNonDefaultFS() throws IOException {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    conf.set("fs.default.name", "invalid://host/bar/foo");
    trashNonDefaultFS(conf);
  }

  public void testTrashEmptier() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass("fs.file.impl", TestLFS.class, FileSystem.class);
    trashEmptier(FileSystem.getLocal(conf), conf);
  }
  
  protected void trashEmptier(FileSystem fs, Configuration conf) throws Exception {
    // Trash with 12 second deletes and 6 seconds checkpoints
    conf.set("fs.trash.interval", "0.2"); // 12 seconds
    conf.set("fs.trash.checkpoint.interval", "0.1"); // 6 seconds
    Trash trash = new Trash(conf);
    // clean up trash can
    fs.delete(trash.getCurrentTrashDir().getParent(), true);

    // Start Emptier in background
    Runnable emptier = trash.getEmptier();
    Thread emptierThread = new Thread(emptier);
    emptierThread.start();

    FsShell shell = new FsShell();
    shell.setConf(conf);
    shell.init();
    // First create a new directory with mkdirs
    Path myPath = new Path(TEST_DIR, "test/mkdirs");
    mkdir(fs, myPath);
    int fileIndex = 0;
    Set<String> checkpoints = new HashSet<String>();
    while (true)  {
      // Create a file with a new name
      Path myFile = new Path(TEST_DIR, "test/mkdirs/myFile" + fileIndex++);
      writeFile(fs, myFile);

      // Delete the file to trash
      String[] args = new String[2];
      args[0] = "-rm";
      args[1] = myFile.toString();
      int val = -1;
      try {
        val = shell.run(args);
      } catch (Exception e) {
        System.err.println("Exception raised from Trash.run " +
                           e.getLocalizedMessage());
      }
      assertTrue(val == 0);

      Path trashDir = shell.getCurrentTrashDir();
      FileStatus files[] = fs.listStatus(trashDir.getParent());
      // Scan files in .Trash and add them to set of checkpoints
      for (FileStatus file : files) {
        String fileName = file.getPath().getName();
        checkpoints.add(fileName);
      }
      // If checkpoints has 5 objects it is Current + 4 checkpoint directories
      if (checkpoints.size() == 5) {
        // The actual contents should be smaller since the last checkpoint
        // should've been deleted and Current might not have been recreated yet
        assertTrue(5 > files.length);
        break;
      }
      Thread.sleep(5000);
    }
    emptierThread.interrupt();
    emptierThread.join();
  }

  static class TestLFS extends LocalFileSystem {
    Path home;
    TestLFS() {
      this(new Path(TEST_DIR, "user/test"));
    }
    TestLFS(Path home) {
      super();
      this.home = home;
    }
    public Path getHomeDirectory() {
      return home;
    }
  }

  // Test TrashPolicy. Don't care about implementation.
  public static class TestTrashPolicy extends TrashPolicy {
    public TestTrashPolicy() { }

    @Override
    public void initialize(Configuration conf, FileSystem fs, Path home) {
    }

    @Override
    public boolean isEnabled() {
      return false;
    }

    @Override
    public boolean moveToTrash(Path path) throws IOException {
      return false;
    }

    @Override
    public void createCheckpoint() throws IOException {
    }

    @Override
    public void deleteCheckpoint() throws IOException {
    }

    @Override
    public Path getCurrentTrashDir() {
      return null;
    }

    @Override
    public Runnable getEmptier() throws IOException {
      return null;
    }
  }
}
