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

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import junit.framework.TestCase;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;


/**
 * A JUnit test for copying files recursively.
 */
public class TestCopyFiles extends TestCase {

  private static final String JT_STAGING_AREA_ROOT = "mapreduce.jobtracker.staging.root.dir";
  private static final String JT_STAGING_AREA_ROOT_DEFAULT = "/tmp/hadoop/mapred/staging";
  private static final String FS_TRASH_INTERVAL_KEY = "fs.trash.interval";

  {
    ((Log4JLogger)LogFactory.getLog("org.apache.hadoop.hdfs.StateChange")
        ).getLogger().setLevel(Level.OFF);
    ((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.OFF);
    ((Log4JLogger)LogFactory.getLog(FSNamesystem.class)
        ).getLogger().setLevel(Level.OFF);
    ((Log4JLogger)DistCp.LOG).getLogger().setLevel(Level.ALL);
  }
  
  private static final String LOCAL_FS_STR = "file:///";
  private static final URI LOCAL_FS_URI = URI.create(LOCAL_FS_STR);

  private static final Random RAN = new Random();
  private static final int NFILES = 20;
  private static String TEST_ROOT_DIR =
    new Path(System.getProperty("test.build.data","/tmp"))
    .toString().replace(' ', '+');

  /** class MyFile contains enough information to recreate the contents of
   * a single file.
   */
  private static class MyFile {
    private static Random gen = new Random();
    private static final int MAX_LEVELS = 3;
    private static final int MAX_SIZE = 8*1024;
    private static String[] dirNames = {
      "zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"
    };
    private final String name;
    private int size = 0;
    private long seed = 0L;

    MyFile() {
      this(gen.nextInt(MAX_LEVELS));
    }
    MyFile(int nLevels) {
      String xname = "";
      if (nLevels != 0) {
        int[] levels = new int[nLevels];
        for (int idx = 0; idx < nLevels; idx++) {
          levels[idx] = gen.nextInt(10);
        }
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < nLevels; idx++) {
          sb.append(dirNames[levels[idx]]);
          sb.append("/");
        }
        xname = sb.toString();
      }
      long fidx = gen.nextLong() & Long.MAX_VALUE;
      name = xname + Long.toString(fidx);
      reset();
    }
    void reset() {
      final int oldsize = size;
      do { size = gen.nextInt(MAX_SIZE); } while (oldsize == size);
      final long oldseed = seed;
      do { seed = gen.nextLong() & Long.MAX_VALUE; } while (oldseed == seed);
    }
    String getName() { return name; }
    int getSize() { return size; }
    long getSeed() { return seed; }
  }

  private static MyFile[] createFiles(URI fsname, String topdir)
    throws IOException {
    return createFiles(FileSystem.get(fsname, new Configuration()), topdir);
  }

  /** create NFILES with random names and directory hierarchies
   * with random (but reproducible) data in them.
   */
  private static MyFile[] createFiles(FileSystem fs, String topdir)
    throws IOException {
    Path root = new Path(topdir);
    MyFile[] files = new MyFile[NFILES];
    for (int i = 0; i < NFILES; i++) {
      files[i] = createFile(root, fs);
    }
    return files;
  }

  static MyFile createFile(Path root, FileSystem fs, int levels)
      throws IOException {
    MyFile f = levels < 0 ? new MyFile() : new MyFile(levels);
    Path p = new Path(root, f.getName());
    FSDataOutputStream out = fs.create(p);
    byte[] toWrite = new byte[f.getSize()];
    new Random(f.getSeed()).nextBytes(toWrite);
    out.write(toWrite);
    out.close();
    FileSystem.LOG.info("created: " + p + ", size=" + f.getSize());
    return f;
  }

  static MyFile createFile(Path root, FileSystem fs) throws IOException {
    return createFile(root, fs, -1);
  }

  private static boolean checkFiles(FileSystem fs, String topdir, MyFile[] files
      ) throws IOException {
    return checkFiles(fs, topdir, files, false);    
  }

  private static boolean checkFiles(FileSystem fs, String topdir, MyFile[] files,
      boolean existingOnly) throws IOException {
    Path root = new Path(topdir);
    
    for (int idx = 0; idx < files.length; idx++) {
      Path fPath = new Path(root, files[idx].getName());
      try {
        fs.getFileStatus(fPath);
        FSDataInputStream in = fs.open(fPath);
        byte[] toRead = new byte[files[idx].getSize()];
        byte[] toCompare = new byte[files[idx].getSize()];
        Random rb = new Random(files[idx].getSeed());
        rb.nextBytes(toCompare);
        assertEquals("Cannnot read file.", toRead.length, in.read(toRead));
        in.close();
        for (int i = 0; i < toRead.length; i++) {
          if (toRead[i] != toCompare[i]) {
            return false;
          }
        }
        toRead = null;
        toCompare = null;
      }
      catch(FileNotFoundException fnfe) {
        if (!existingOnly) {
          throw fnfe;
        }
      }
    }
    
    return true;
  }

  private static void updateFiles(FileSystem fs, String topdir, MyFile[] files,
        int nupdate) throws IOException {
    assert nupdate <= NFILES;

    Path root = new Path(topdir);

    for (int idx = 0; idx < nupdate; ++idx) {
      Path fPath = new Path(root, files[idx].getName());
      // overwrite file
      assertTrue(fPath.toString() + " does not exist", fs.exists(fPath));
      FSDataOutputStream out = fs.create(fPath);
      files[idx].reset();
      byte[] toWrite = new byte[files[idx].getSize()];
      Random rb = new Random(files[idx].getSeed());
      rb.nextBytes(toWrite);
      out.write(toWrite);
      out.close();
    }
  }

  private static FileStatus[] getFileStatus(FileSystem fs,
      String topdir, MyFile[] files) throws IOException {
    return getFileStatus(fs, topdir, files, false);
  }
  private static FileStatus[] getFileStatus(FileSystem fs,
      String topdir, MyFile[] files, boolean existingOnly) throws IOException {
    Path root = new Path(topdir);
    List<FileStatus> statuses = new ArrayList<FileStatus>();
    for (int idx = 0; idx < NFILES; ++idx) {
      try {
        statuses.add(fs.getFileStatus(new Path(root, files[idx].getName())));
      } catch(FileNotFoundException fnfe) {
        if (!existingOnly) {
          throw fnfe;
        }
      }
    }
    return statuses.toArray(new FileStatus[statuses.size()]);
  }

  private static boolean checkUpdate(FileSystem fs, FileStatus[] old,
      String topdir, MyFile[] upd, final int nupdate) throws IOException {
    Path root = new Path(topdir);

    // overwrote updated files
    for (int idx = 0; idx < nupdate; ++idx) {
      final FileStatus stat =
        fs.getFileStatus(new Path(root, upd[idx].getName()));
      if (stat.getModificationTime() <= old[idx].getModificationTime()) {
        return false;
      }
    }
    // did not overwrite files not updated
    for (int idx = nupdate; idx < NFILES; ++idx) {
      final FileStatus stat =
        fs.getFileStatus(new Path(root, upd[idx].getName()));
      if (stat.getModificationTime() != old[idx].getModificationTime()) {
        return false;
      }
    }
    return true;
  }

  /** delete directory and everything underneath it.*/
  private static void deldir(FileSystem fs, String topdir) throws IOException {
    fs.delete(new Path(topdir), true);
  }
  
  /** copy files from local file system to local file system */
  public void testCopyFromLocalToLocal() throws Exception {
    Configuration conf = new Configuration();
    FileSystem localfs = FileSystem.get(LOCAL_FS_URI, conf);
    MyFile[] files = createFiles(LOCAL_FS_URI, TEST_ROOT_DIR+"/srcdat");
    ToolRunner.run(new DistCp(new Configuration()),
                           new String[] {LOCAL_FS_STR+TEST_ROOT_DIR+"/srcdat",
                                         LOCAL_FS_STR+TEST_ROOT_DIR+"/destdat"});
    assertTrue("Source and destination directories do not match.",
               checkFiles(localfs, TEST_ROOT_DIR+"/destdat", files));
    deldir(localfs, TEST_ROOT_DIR+"/destdat");
    deldir(localfs, TEST_ROOT_DIR+"/srcdat");
  }

  /** copy files from local file system to local file system
   *  using relative short path name to verify
   *  DistCp supports relative short path name */
  public void testCopyFromLocalToLocalUsingRelativePathName() throws Exception {
    Configuration conf = new Configuration();
    FileSystem localfs = FileSystem.get(LOCAL_FS_URI, conf);
    MyFile[] files = createFiles(LOCAL_FS_URI, TEST_ROOT_DIR+"/srcdat");

    // get DistCp source and destination relative path
    Path src = new Path(TEST_ROOT_DIR+"/srcdat");
    Path srcRoot = new Path(".");
    FileStatus srcfilestat = localfs.getFileStatus(src);
    FileStatus srcRootfilestat = localfs.getFileStatus(srcRoot);
    // srcStr is file:/{hadoop-src-root}/build/test/data/srcdat
    String srcStr = srcfilestat.getPath().toString();
    // srcRootStr is file:/{hadoop-src-root}
    String srcRootStr = srcRootfilestat.getPath().toString();
    // +1 to remove /, srcRelativePath is build/test/data/srcdat
    String srcRelativePath = srcStr.substring(srcRootStr.length() + 1);
    // replace "srcdat" with "destdat" to get destRelativePath,
    // destRelativePath is build/test/data/destdat
    String destRelativePath = srcRelativePath.substring(0,
        srcRelativePath.length() - "srcdat".length()) + "destdat";

    // run DistCp with relative path source and destination parameters
    ToolRunner.run(new DistCp(new Configuration()),
        new String[] {srcRelativePath, destRelativePath});

    // check result
    assertTrue("Source and destination directories do not match.",
        checkFiles(localfs, TEST_ROOT_DIR+"/destdat", files));

    // clean up
    deldir(localfs, TEST_ROOT_DIR+"/destdat");
    deldir(localfs, TEST_ROOT_DIR+"/srcdat");
  }

  private static void addToArgList(List<String> argList, final String... args) {
	for (String arg : args) {
	  argList.add(arg);
	}
  }
  
  private void addSrcDstToArgList(List<String> argList, final boolean skipTmp,
      final String dst, final String... srcs) {
    if (skipTmp) {
      argList.add("-skiptmp");
    }
    addToArgList(argList, srcs);
    argList.add(dst);
  }

  /**
   * copy files from dfs file system to dfs file system Pass option to use
   * -skiptmp flag
   */
  private void testCopyFromDfsToDfs(boolean skipTmp) throws Exception {
    String namenode = null;
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        List<String> argList = new ArrayList<String>();
        addToArgList(argList, "-log", namenode + "/logs");
        addSrcDstToArgList(argList, skipTmp, namenode + "/destdat", namenode
            + "/srcdat");
        ToolRunner.run(new DistCp(conf),
            argList.toArray(new String[argList.size()]));
        assertTrue("Source and destination directories do not match.",
            checkFiles(hdfs, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode + "/logs"), conf);
        assertTrue("Log directory does not exist.",
            fs.exists(new Path(namenode + "/logs")));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /** copy files from dfs file system to dfs file system */
  public void testCopyFromDfsToDfs() throws Exception {
    testCopyFromDfsToDfs(false);
  }

  /** copy files from dfs file system to dfs file system with skiptmp */
  public void testCopyFromDfsToDfsWithSkiptmp() throws Exception {
    testCopyFromDfsToDfs(true);
  }
  
  /** copy files from local file system to dfs file system */
  public void testCopyFromLocalToDfs() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 1, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(LOCAL_FS_URI, TEST_ROOT_DIR+"/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-log",
                                         namenode+"/logs",
                                         LOCAL_FS_STR+TEST_ROOT_DIR+"/srcdat",
                                         namenode+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(cluster.getFileSystem(), "/destdat", files));
        assertTrue("Log directory does not exist.",
                    hdfs.exists(new Path(namenode+"/logs")));
        deldir(hdfs, "/destdat");
        deldir(hdfs, "/logs");
        deldir(FileSystem.get(LOCAL_FS_URI, conf), TEST_ROOT_DIR+"/srcdat");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /** copy files from dfs file system to local file system */
  public void testCopyFromDfsToLocal() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      final FileSystem localfs = FileSystem.get(LOCAL_FS_URI, conf);
      cluster = new MiniDFSCluster(conf, 1, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-log",
                                         "/logs",
                                         namenode+"/srcdat",
                                         LOCAL_FS_STR+TEST_ROOT_DIR+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(localfs, TEST_ROOT_DIR+"/destdat", files));
        assertTrue("Log directory does not exist.",
                    hdfs.exists(new Path("/logs")));
        deldir(localfs, TEST_ROOT_DIR+"/destdat");
        deldir(hdfs, "/logs");
        deldir(hdfs, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /**
   * verify that -delete option works for other {@link FileSystem}
   * implementations. See MAPREDUCE-1285 */
  public void testDeleteLocal() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      final FileSystem localfs = FileSystem.get(LOCAL_FS_URI, conf);
      cluster = new MiniDFSCluster(conf, 1, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = FileSystem.getDefaultUri(conf).toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        String destdir = TEST_ROOT_DIR + "/destdat";
        MyFile[] localFiles = createFiles(localfs, destdir);
        ToolRunner.run(new DistCp(conf), new String[] {
                                         "-delete",
                                         "-update",
                                         "-log",
                                         "/logs",
                                         namenode+"/srcdat",
                                         LOCAL_FS_STR+TEST_ROOT_DIR+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(localfs, destdir, files));
        assertTrue("Log directory does not exist.",
                    hdfs.exists(new Path("/logs")));
        deldir(localfs, destdir);
        deldir(hdfs, "/logs");
        deldir(hdfs, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  private void testCopyDfsToDfsUpdateOverwrite(boolean skipTmp)
      throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();
      if (namenode.startsWith("hdfs://")) {
        MyFile[] files = createFiles(URI.create(namenode), "/srcdat");
        // run without update
        List<String> argList = new ArrayList<String>();
        addToArgList(argList, "-p", "-log", namenode + "/logs");
        addSrcDstToArgList(argList, skipTmp, namenode + "/destdat", namenode
            + "/srcdat");
        ToolRunner.run(new DistCp(conf),
            argList.toArray(new String[argList.size()]));
        assertTrue("Source and destination directories do not match.",
            checkFiles(hdfs, "/destdat", files));
        FileSystem fs = FileSystem.get(URI.create(namenode + "/logs"), conf);
        assertTrue("Log directory does not exist.",
            fs.exists(new Path(namenode + "/logs")));

        FileStatus[] dchkpoint = getFileStatus(hdfs, "/destdat", files);
        final int nupdate = NFILES >> 2;
        updateFiles(cluster.getFileSystem(), "/srcdat", files, nupdate);
        deldir(hdfs, "/logs");
        argList.clear();
        // run with update
        addToArgList(argList, "-p", "-update", "-log", namenode + "/logs");
        addSrcDstToArgList(argList, skipTmp, namenode + "/destdat", namenode
            + "/srcdat");
        ToolRunner.run(new DistCp(conf),
            argList.toArray(new String[argList.size()]));
        assertTrue("Source and destination directories do not match.",
            checkFiles(hdfs, "/destdat", files));
        assertTrue("Update failed to replicate all changes in src",
            checkUpdate(hdfs, dchkpoint, "/destdat", files, nupdate));

        deldir(hdfs, "/logs");
        argList.clear();
        // run with overwrite
        addToArgList(argList, "-p", "-overwrite", "-log", namenode + "/logs");
        addSrcDstToArgList(argList, skipTmp, namenode + "/destdat", namenode
            + "/srcdat");
        ToolRunner.run(new DistCp(conf),
            argList.toArray(new String[argList.size()]));
        assertTrue("Source and destination directories do not match.",
            checkFiles(hdfs, "/destdat", files));
        assertTrue("-overwrite didn't.",
            checkUpdate(hdfs, dchkpoint, "/destdat", files, NFILES));

        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public void testCopyDfsToDfsUpdateOverwrite() throws Exception {
    testCopyDfsToDfsUpdateOverwrite(false);
  }

  public void testCopyDfsToDfsUpdateOverwriteSkiptmp() throws Exception {
    testCopyDfsToDfsUpdateOverwrite(true);
  }

  private void testCopyDfsToDfsUpdateWithSkipCRC(boolean skipTmp)
      throws Exception {
    MiniDFSCluster cluster = null;
    try {
      Configuration conf = new Configuration();
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final FileSystem hdfs = cluster.getFileSystem();
      final String namenode = hdfs.getUri().toString();

      FileSystem fs = FileSystem.get(URI.create(namenode), new Configuration());
      // Create two files of the same name, same length but different
      // contents
      final String testfilename = "test";
      final String srcData = "act act act";
      final String destData = "cat cat cat";

      if (namenode.startsWith("hdfs://")) {
        deldir(hdfs, "/logs");

        Path srcPath = new Path("/srcdat", testfilename);
        Path destPath = new Path("/destdat", testfilename);
        FSDataOutputStream out = fs.create(srcPath, true);
        out.writeUTF(srcData);
        out.close();

        out = fs.create(destPath, true);
        out.writeUTF(destData);
        out.close();
        // Run with -skipcrccheck option
        List<String> argList = new ArrayList<String>();
        addToArgList(argList, "-p", "-update", "-skipcrccheck", "-log", namenode + "/logs");
        addSrcDstToArgList(argList, skipTmp, namenode + "/destdat", namenode
            + "/srcdat");
        ToolRunner.run(new DistCp(conf),
            argList.toArray(new String[argList.size()]));
        // File should not be overwritten
        FSDataInputStream in = hdfs.open(destPath);
        String s = in.readUTF();
        System.out.println("Dest had: " + s);
        assertTrue("Dest got over written even with skip crc",
            s.equalsIgnoreCase(destData));
        in.close();
        deldir(hdfs, "/logs");
        argList.clear();
        // Run without the option
        addToArgList(argList, "-p", "-update", "-log", namenode + "/logs");
        addSrcDstToArgList(argList, skipTmp, namenode + "/destdat", namenode
            + "/srcdat");
        ToolRunner.run(new DistCp(conf),
            argList.toArray(new String[argList.size()]));
        // File should be overwritten
        in = hdfs.open(destPath);
        s = in.readUTF();
        System.out.println("Dest had: " + s);

        assertTrue("Dest did not get overwritten without skip crc",
            s.equalsIgnoreCase(srcData));
        in.close();

        deldir(hdfs, "/destdat");
        deldir(hdfs, "/srcdat");
        deldir(hdfs, "/logs");
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  public void testCopyDfsToDfsUpdateWithSkipCRC() throws Exception {
    testCopyDfsToDfsUpdateWithSkipCRC(false);
  }

  public void testCopyDfsToDfsUpdateWithSkipCRCSkiptmp() throws Exception {
    testCopyDfsToDfsUpdateWithSkipCRC(true);
  }

  /**
   * A helper function to test copying files between local file system and dfs
   * file system, with staging area set to local file system.
   */
  private void stagingAreaTest(final FileSystem srcFs, final FileSystem destFs,
      MiniDFSCluster cluster, Configuration conf, boolean skipTmp)
      throws Exception {
    try {
      final String fileDir = "/files";
      final String srcParent = "/srcdat";
      final String destParent = "/destdata";
      final String source = srcParent + fileDir;
      final String destination = destParent + fileDir;
      final String logs = "/logs";
      String logDir = TEST_ROOT_DIR + logs;

      URI srcUri = srcFs.getUri();
      URI destUri = destFs.getUri();

      final boolean isSrcLocalFs =
          srcUri.getScheme().equals(LOCAL_FS_URI.getScheme());

      final FileSystem localFs = FileSystem.get(LOCAL_FS_URI, conf);
      String prevStagingArea =
          conf.get(JT_STAGING_AREA_ROOT, JT_STAGING_AREA_ROOT_DEFAULT);
      String newStagingArea = (isSrcLocalFs ? source : destination);
      newStagingArea += "/STAGING";
      conf.set(JT_STAGING_AREA_ROOT, TEST_ROOT_DIR + newStagingArea);

      final String srcParentPrefix = isSrcLocalFs ? TEST_ROOT_DIR : "";
      final String destParentPrefix = isSrcLocalFs ? "" : TEST_ROOT_DIR;

      String createDelSrcParent = srcParentPrefix + srcParent;
      String createDelDestParent = destParentPrefix + destParent;
      String createDelSrc = createDelSrcParent + fileDir;
      String createDelDest = createDelDestParent + fileDir;

      MyFile[] srcFiles = createFiles(srcUri, createDelSrc);
      createFiles(destUri, createDelDest);

      String distcpSrc = String.valueOf(srcUri) + createDelSrc;
      String distcpDest = String.valueOf(destUri) + createDelDest;

      List<String> argList = new ArrayList<String>();
      addToArgList(argList, "-log", LOCAL_FS_STR + logDir, "-update", "-delete");
      addSrcDstToArgList(argList, skipTmp, distcpDest, distcpSrc);
      ToolRunner.run(new DistCp(conf),
          argList.toArray(new String[argList.size()]));

      assertTrue("Source and destination directories do not match.",
          checkFiles(destFs, createDelDest, srcFiles));

      deldir(localFs, logDir);
      deldir(srcFs, createDelSrcParent);
      deldir(destFs, createDelDestParent);

      conf.set(JT_STAGING_AREA_ROOT, prevStagingArea);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }

  /**
   * test copying files from local file system to dfs file system with staging
   * area in src
   */
  private void testCopyFromLocalToDfsWithStagingAreaInSrc(boolean skipTmp)
      throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);

    String namenode = FileSystem.getDefaultUri(conf).toString();
    assertTrue("Name node doesn't start with hdfs://",
        namenode.startsWith("hdfs://"));

    final FileSystem srcFs = FileSystem.get(LOCAL_FS_URI, conf);
    final FileSystem destFs = cluster.getFileSystem();

    stagingAreaTest(srcFs, destFs, cluster, conf, skipTmp);
  }

  public void testCopyFromLocalToDfsWithStagingAreaInSrcSkiptmp()
      throws Exception {
    testCopyFromLocalToDfsWithStagingAreaInSrc(true);
  }

  public void testCopyFromLocalToDfsWithStagingAreaInSrc() throws Exception {
    testCopyFromLocalToDfsWithStagingAreaInSrc(false);
  }

  /**
   * test copying files from dfs file system to local file system with staging
   * area in dest and setting skiptmp flag as needed
   */
  public void testCopyFromDfsToLocalWithStagingAreaInDest(boolean skipTmp)
      throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = new MiniDFSCluster(conf, 2, true, null);

    String namenode = FileSystem.getDefaultUri(conf).toString();
    assertTrue("Name node doesn't start with hdfs://",
        namenode.startsWith("hdfs://"));

    final FileSystem srcFs = cluster.getFileSystem();
    final FileSystem destFs = FileSystem.get(LOCAL_FS_URI, conf);

    stagingAreaTest(srcFs, destFs, cluster, conf, skipTmp);
  }
  
  
  /**
   * test copying files from dfs file system to local file system with staging
   * area in dest and skiptmp set
   */
  public void testCopyFromDfsToLocalWithStagingAreaInDestSkiptmp()
    throws Exception {
	testCopyFromDfsToLocalWithStagingAreaInDest(true);
  }

  /**
   * test copying files from dfs file system to local file system with staging
   * area in dest
   */
  public void testCopyFromDfsToLocalWithStagingAreaInDest() throws Exception {
	testCopyFromDfsToLocalWithStagingAreaInDest(false);
  }

  /**
   * test copying files from dfs file system to local file system with staging
   * area in dest. Optionally set skiptmp flag
   */
  private void testCopyDuplication(boolean skipTmp) throws Exception {
    final FileSystem localfs =
        FileSystem.get(LOCAL_FS_URI, new Configuration());
    try {
      MyFile[] files = createFiles(localfs, TEST_ROOT_DIR + "/srcdat");
      List<String> argList = new ArrayList<String>();
      addSrcDstToArgList(argList, skipTmp, LOCAL_FS_STR + TEST_ROOT_DIR
          + "/src2/srcdat", LOCAL_FS_STR + TEST_ROOT_DIR + "/srcdat");
      ToolRunner.run(new DistCp(new Configuration()),
          argList.toArray(new String[argList.size()]));
      assertTrue("Source and destination directories do not match.",
          checkFiles(localfs, TEST_ROOT_DIR + "/src2/srcdat", files));
      argList.clear();
      
      addSrcDstToArgList(argList, skipTmp, LOCAL_FS_STR + TEST_ROOT_DIR
          + "/destdat", LOCAL_FS_STR + TEST_ROOT_DIR + "/srcdat",
          LOCAL_FS_STR + TEST_ROOT_DIR + "/src2/srcdat");
      assertEquals(
          DistCp.DuplicationException.ERROR_CODE,
          ToolRunner.run(new DistCp(new Configuration()),
              argList.toArray(new String[argList.size()])));
    } finally {
      deldir(localfs, TEST_ROOT_DIR + "/destdat");
      deldir(localfs, TEST_ROOT_DIR + "/srcdat");
      deldir(localfs, TEST_ROOT_DIR + "/src2");
    }
  }

  public void testCopyDuplication() throws Exception {
    testCopyDuplication(false);
  }

  public void testCopyDuplicationSkiptmp() throws Exception {
    testCopyDuplication(true);
  }

  public void oldtestCopyDuplication() throws Exception {
    final FileSystem localfs = FileSystem.get(LOCAL_FS_URI, new Configuration());
    try {    
      MyFile[] files = createFiles(localfs, TEST_ROOT_DIR+"/srcdat");
      ToolRunner.run(new DistCp(new Configuration()),
          new String[] {LOCAL_FS_STR+TEST_ROOT_DIR+"/srcdat",
                        LOCAL_FS_STR+TEST_ROOT_DIR+"/src2/srcdat"});
      assertTrue("Source and destination directories do not match.",
                 checkFiles(localfs, TEST_ROOT_DIR+"/src2/srcdat", files));
  
      assertEquals(DistCp.DuplicationException.ERROR_CODE,
          ToolRunner.run(new DistCp(new Configuration()),
          new String[] {LOCAL_FS_STR+TEST_ROOT_DIR+"/srcdat",
                        LOCAL_FS_STR+TEST_ROOT_DIR+"/src2/srcdat",
                        LOCAL_FS_STR+TEST_ROOT_DIR+"/destdat",}));
    }
    finally {
      deldir(localfs, TEST_ROOT_DIR+"/destdat");
      deldir(localfs, TEST_ROOT_DIR+"/srcdat");
      deldir(localfs, TEST_ROOT_DIR+"/src2");
    }
  }

  private void testCopySingleFile(boolean skipTmp) throws Exception {
    FileSystem fs = FileSystem.get(LOCAL_FS_URI, new Configuration());
    Path root = new Path(TEST_ROOT_DIR + "/srcdat");
    try {
      MyFile[] files = { createFile(root, fs) };
      List<String> argList = new ArrayList<String>();
      // copy a dir with a single file
      addSrcDstToArgList(argList, skipTmp, LOCAL_FS_STR + TEST_ROOT_DIR
          + "/destdat", LOCAL_FS_STR + TEST_ROOT_DIR + "/srcdat");
      ToolRunner.run(new DistCp(new Configuration()),
          argList.toArray(new String[argList.size()]));
      assertTrue("Source and destination directories do not match.",
          checkFiles(fs, TEST_ROOT_DIR + "/destdat", files));
      argList.clear();
      // copy a single file
      String fname = files[0].getName();
      Path p = new Path(root, fname);
      FileSystem.LOG.info("fname=" + fname + ", exists? " + fs.exists(p));
      addSrcDstToArgList(argList, skipTmp, LOCAL_FS_STR + TEST_ROOT_DIR
          + "/dest2/" + fname, LOCAL_FS_STR + TEST_ROOT_DIR + "/srcdat/"
          + fname);
      ToolRunner.run(new DistCp(new Configuration()),
          argList.toArray(new String[argList.size()]));
      assertTrue("Source and destination directories do not match.",
          checkFiles(fs, TEST_ROOT_DIR + "/dest2", files));
      argList.clear();
      // copy single file to existing dir
      deldir(fs, TEST_ROOT_DIR + "/dest2");
      fs.mkdirs(new Path(TEST_ROOT_DIR + "/dest2"));
      MyFile[] files2 = { createFile(root, fs, 0) };
      String sname = files2[0].getName();
      addToArgList(argList, "-update");
      addSrcDstToArgList(argList, skipTmp, LOCAL_FS_STR + TEST_ROOT_DIR
          + "/dest2/", LOCAL_FS_STR + TEST_ROOT_DIR + "/srcdat/" + sname);
      ToolRunner.run(new DistCp(new Configuration()),
          argList.toArray(new String[argList.size()]));
      assertTrue("Source and destination directories do not match.",
          checkFiles(fs, TEST_ROOT_DIR + "/dest2", files2));
      updateFiles(fs, TEST_ROOT_DIR + "/srcdat", files2, 1);
      argList.clear();
      // copy single file to existing dir w/ dst name conflict
      addToArgList(argList, "-update");
      addSrcDstToArgList(argList, skipTmp, LOCAL_FS_STR + TEST_ROOT_DIR
          + "/dest2/", LOCAL_FS_STR + TEST_ROOT_DIR + "/srcdat/" + sname);
      ToolRunner.run(new DistCp(new Configuration()),
          argList.toArray(new String[argList.size()]));
      assertTrue("Source and destination directories do not match.",
          checkFiles(fs, TEST_ROOT_DIR + "/dest2", files2));
    } finally {
      deldir(fs, TEST_ROOT_DIR + "/destdat");
      deldir(fs, TEST_ROOT_DIR + "/dest2");
      deldir(fs, TEST_ROOT_DIR + "/srcdat");
    }
  }

  public void testCopySingleFile() throws Exception {
    testCopySingleFile(false);
  }

  public void testCopySingleFileWithSkiptmp() throws Exception {
    testCopySingleFile(true);
  }
  
  
  public void testPreserveOption() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      String nnUri = FileSystem.getDefaultUri(conf).toString();
      FileSystem fs = FileSystem.get(URI.create(nnUri), conf);

      {//test preserving user
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        for(int i = 0; i < srcstat.length; i++) {
          fs.setOwner(srcstat[i].getPath(), "u" + i, null);
        }
        ToolRunner.run(new DistCp(conf),
            new String[]{"-pu", nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));
        
        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, "u" + i, dststat[i].getOwner());
        }
        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }

      {//test preserving group
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        for(int i = 0; i < srcstat.length; i++) {
          fs.setOwner(srcstat[i].getPath(), null, "g" + i);
        }
        ToolRunner.run(new DistCp(conf),
            new String[]{"-pg", nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));
        
        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, "g" + i, dststat[i].getGroup());
        }
        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }

      {//test preserving mode
        MyFile[] files = createFiles(URI.create(nnUri), "/srcdat");
        FileStatus[] srcstat = getFileStatus(fs, "/srcdat", files);
        FsPermission[] permissions = new FsPermission[srcstat.length];
        for(int i = 0; i < srcstat.length; i++) {
          permissions[i] = new FsPermission((short)(i & 0666));
          fs.setPermission(srcstat[i].getPath(), permissions[i]);
        }

        ToolRunner.run(new DistCp(conf),
            new String[]{"-pp", nnUri+"/srcdat", nnUri+"/destdat"});
        assertTrue("Source and destination directories do not match.",
                   checkFiles(fs, "/destdat", files));
  
        FileStatus[] dststat = getFileStatus(fs, "/destdat", files);
        for(int i = 0; i < dststat.length; i++) {
          assertEquals("i=" + i, permissions[i], dststat[i].getPermission());
        }
        deldir(fs, "/destdat");
        deldir(fs, "/srcdat");
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  public void testMapCount() throws Exception {
    String namenode = null;
    MiniDFSCluster dfs = null;
    MiniMRCluster mr = null;
    try {
      Configuration conf = new Configuration();
      dfs = new MiniDFSCluster(conf, 3, true, null);
      FileSystem fs = dfs.getFileSystem();
      final FsShell shell = new FsShell(conf);
      namenode = fs.getUri().toString();
      mr = new MiniMRCluster(3, namenode, 1);
      MyFile[] files = createFiles(fs.getUri(), "/srcdat");
      long totsize = 0;
      for (MyFile f : files) {
        totsize += f.getSize();
      }
      Configuration job = mr.createJobConf();
      job.setLong("distcp.bytes.per.map", totsize / 3);
      ToolRunner.run(new DistCp(job),
          new String[] {"-m", "100",
                        "-log",
                        namenode+"/logs",
                        namenode+"/srcdat",
                        namenode+"/destdat"});
      assertTrue("Source and destination directories do not match.",
                 checkFiles(fs, "/destdat", files));

      String logdir = namenode + "/logs";
      System.out.println(execCmd(shell, "-lsr", logdir));
      FileStatus[] logs = fs.listStatus(new Path(logdir));
      // rare case where splits are exact, logs.length can be 4
      assertTrue("Unexpected map count, logs.length=" + logs.length,
          logs.length == 5 || logs.length == 4);

      deldir(fs, "/destdat");
      deldir(fs, "/logs");
      ToolRunner.run(new DistCp(job),
          new String[] {"-m", "1",
                        "-log",
                        namenode+"/logs",
                        namenode+"/srcdat",
                        namenode+"/destdat"});

      System.out.println(execCmd(shell, "-lsr", logdir));
      logs = fs.listStatus(new Path(namenode+"/logs"));
      assertTrue("Unexpected map count, logs.length=" + logs.length,
          logs.length == 2);
    } finally {
      if (dfs != null) { dfs.shutdown(); }
      if (mr != null) { mr.shutdown(); }
    }
  }

  public void testLimits() throws Exception {
    Configuration conf = new Configuration();
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final String nnUri = FileSystem.getDefaultUri(conf).toString();
      final FileSystem fs = FileSystem.get(URI.create(nnUri), conf);
      final DistCp distcp = new DistCp(conf);
      final FsShell shell = new FsShell(conf);  

      final String srcrootdir =  "/src_root";
      final Path srcrootpath = new Path(srcrootdir); 
      final String dstrootdir =  "/dst_root";
      final Path dstrootpath = new Path(dstrootdir); 

      {//test -filelimit
        MyFile[] files = createFiles(URI.create(nnUri), srcrootdir);
        int filelimit = files.length / 2;
        System.out.println("filelimit=" + filelimit);

        ToolRunner.run(distcp,
            new String[]{"-filelimit", ""+filelimit, nnUri+srcrootdir, nnUri+dstrootdir});
        String results = execCmd(shell, "-lsr", dstrootdir);
        results = removePrefix(results, dstrootdir);
        System.out.println("results=" +  results);

        FileStatus[] dststat = getFileStatus(fs, dstrootdir, files, true);
        assertEquals(filelimit, dststat.length);
        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }

      {//test -sizelimit
        createFiles(URI.create(nnUri), srcrootdir);
        long sizelimit = fs.getContentSummary(srcrootpath).getLength()/2;
        System.out.println("sizelimit=" + sizelimit);

        ToolRunner.run(distcp,
            new String[]{"-sizelimit", ""+sizelimit, nnUri+srcrootdir, nnUri+dstrootdir});
        
        ContentSummary summary = fs.getContentSummary(dstrootpath);
        System.out.println("summary=" + summary);
        assertTrue(summary.getLength() <= sizelimit);
        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }

      {//test update
        final MyFile[] srcs = createFiles(URI.create(nnUri), srcrootdir);
        final long totalsize = fs.getContentSummary(srcrootpath).getLength();
        System.out.println("src.length=" + srcs.length);
        System.out.println("totalsize =" + totalsize);
        fs.mkdirs(dstrootpath);
        final int parts = RAN.nextInt(NFILES/3 - 1) + 2;
        final int filelimit = srcs.length/parts;
        final long sizelimit = totalsize/parts;
        System.out.println("filelimit=" + filelimit);
        System.out.println("sizelimit=" + sizelimit);
        System.out.println("parts    =" + parts);
        final String[] args = {"-filelimit", ""+filelimit, "-sizelimit", ""+sizelimit,
            "-update", nnUri+srcrootdir, nnUri+dstrootdir};

        int dstfilecount = 0;
        long dstsize = 0;
        for(int i = 0; i <= parts; i++) {
          ToolRunner.run(distcp, args);
        
          FileStatus[] dststat = getFileStatus(fs, dstrootdir, srcs, true);
          System.out.println(i + ") dststat.length=" + dststat.length);
          assertTrue(dststat.length - dstfilecount <= filelimit);
          ContentSummary summary = fs.getContentSummary(dstrootpath);
          System.out.println(i + ") summary.getLength()=" + summary.getLength());
          assertTrue(summary.getLength() - dstsize <= sizelimit);
          assertTrue(checkFiles(fs, dstrootdir, srcs, true));
          dstfilecount = dststat.length;
          dstsize = summary.getLength();
        }

        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  static final long now = System.currentTimeMillis();

  static UserGroupInformation createUGI(String name, boolean issuper) {
    String username = name + now;
    String group = issuper? "supergroup": username;
    return UserGroupInformation.createUserForTesting(username, 
        new String[]{group});
  }

  static Path createHomeDirectory(FileSystem fs, UserGroupInformation ugi
      ) throws IOException {
    final Path home = new Path("/user/" + ugi.getUserName());
    fs.mkdirs(home);
    fs.setOwner(home, ugi.getUserName(), ugi.getGroupNames()[0]);
    fs.setPermission(home, new FsPermission((short)0700));
    return home;
  }

  public void testHftpAccessControl() throws Exception {
    MiniDFSCluster cluster = null;
    try {
      final UserGroupInformation DFS_UGI = createUGI("dfs", true); 
      final UserGroupInformation USER_UGI = createUGI("user", false); 

      //start cluster by DFS_UGI
      final Configuration dfsConf = new Configuration();
      cluster = new MiniDFSCluster(dfsConf, 2, true, null);
      cluster.waitActive();

      final String httpAdd = dfsConf.get("dfs.http.address");
      final URI nnURI = FileSystem.getDefaultUri(dfsConf);
      final String nnUri = nnURI.toString();
      FileSystem fs1 = DFS_UGI.doAs(new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws IOException {
          return FileSystem.get(nnURI, dfsConf);
        }
      });
      final Path home = 
        createHomeDirectory(fs1, USER_UGI);
      
      //now, login as USER_UGI
      final Configuration userConf = new Configuration();
      final FileSystem fs = 
        USER_UGI.doAs(new PrivilegedExceptionAction<FileSystem>() {
        public FileSystem run() throws IOException {
          return FileSystem.get(nnURI, userConf);
        }
      });
      
      final Path srcrootpath = new Path(home, "src_root"); 
      final String srcrootdir =  srcrootpath.toString();
      final Path dstrootpath = new Path(home, "dst_root"); 
      final String dstrootdir =  dstrootpath.toString();
      final DistCp distcp = USER_UGI.doAs(new PrivilegedExceptionAction<DistCp>() {
        public DistCp run() {
          return new DistCp(userConf);
        }
      });

      FileSystem.mkdirs(fs, srcrootpath, new FsPermission((short)0700));
      final String[] args = {"hftp://"+httpAdd+srcrootdir, nnUri+dstrootdir};

      { //copy with permission 000, should fail
        fs.setPermission(srcrootpath, new FsPermission((short)0));
        USER_UGI.doAs(new PrivilegedExceptionAction<Void>() {
          public Void run() throws Exception {
            assertEquals(-3, ToolRunner.run(distcp, args));
            return null;
          }
        });
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  /** test -delete */
  public void testDelete() throws Exception {
    final Configuration conf = new Configuration();
    conf.setInt(FS_TRASH_INTERVAL_KEY, 60);
    MiniDFSCluster cluster = null;
    try {
      cluster = new MiniDFSCluster(conf, 2, true, null);
      final URI nnURI = FileSystem.getDefaultUri(conf);
      final String nnUri = nnURI.toString();
      final FileSystem fs = FileSystem.get(URI.create(nnUri), conf);

      final DistCp distcp = new DistCp(conf);
      final FsShell shell = new FsShell(conf);  

      final String srcrootdir = "/src_root";
      final String dstrootdir = "/dst_root";

      {
        //create source files
        createFiles(nnURI, srcrootdir);
        String srcresults = execCmd(shell, "-lsr", srcrootdir);
        srcresults = removePrefix(srcresults, srcrootdir);
        System.out.println("srcresults=" +  srcresults);

        //create some files in dst
        createFiles(nnURI, dstrootdir);
        System.out.println("dstrootdir=" +  dstrootdir);
        shell.run(new String[]{"-lsr", dstrootdir});

        //run distcp
        ToolRunner.run(distcp,
            new String[]{"-delete", "-update", "-log", "/log",
                         nnUri+srcrootdir, nnUri+dstrootdir});

        //make sure src and dst contains the same files
        String dstresults = execCmd(shell, "-lsr", dstrootdir);
        dstresults = removePrefix(dstresults, dstrootdir);
        System.out.println("first dstresults=" +  dstresults);
        assertEquals(srcresults, dstresults);

        //create additional file in dst
        create(fs, new Path(dstrootdir, "foo"));
        create(fs, new Path(dstrootdir, "foobar"));

        //run distcp again
        ToolRunner.run(distcp,
            new String[]{"-delete", "-update", "-log", "/log2",
                         nnUri+srcrootdir, nnUri+dstrootdir});
        
        //make sure src and dst contains the same files
        dstresults = execCmd(shell, "-lsr", dstrootdir);
        dstresults = removePrefix(dstresults, dstrootdir);
        System.out.println("second dstresults=" +  dstresults);
        assertEquals(srcresults, dstresults);
        // verify that files removed in -delete were moved to the trash
        // regrettably, this test will break if Trash changes incompatibly
        assertTrue(fs.exists(new Path(fs.getHomeDirectory(),
                ".Trash/Current" + dstrootdir + "/foo")));
        assertTrue(fs.exists(new Path(fs.getHomeDirectory(),
                ".Trash/Current" + dstrootdir + "/foobar")));

        //cleanup
        deldir(fs, dstrootdir);
        deldir(fs, srcrootdir);
      }
    } finally {
      if (cluster != null) { cluster.shutdown(); }
    }
  }

  static void create(FileSystem fs, Path f) throws IOException {
    FSDataOutputStream out = fs.create(f);
    try {
      byte[] b = new byte[1024 + RAN.nextInt(1024)];
      RAN.nextBytes(b);
      out.write(b);
    } finally {
      if (out != null) out.close();
    }
  }
  
  static String execCmd(FsShell shell, String... args) throws Exception {
    ByteArrayOutputStream baout = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baout, true);
    PrintStream old = System.out;
    System.setOut(out);
    shell.run(args);
    out.close();
    System.setOut(old);
    return baout.toString();
  }
  
  private static String removePrefix(String lines, String prefix) {
    final int prefixlen = prefix.length();
    final StringTokenizer t = new StringTokenizer(lines, "\n");
    final StringBuffer results = new StringBuffer(); 
    for(; t.hasMoreTokens(); ) {
      String s = t.nextToken();
      results.append(s.substring(s.indexOf(prefix) + prefixlen) + "\n");
    }
    return results.toString();
  }
}
