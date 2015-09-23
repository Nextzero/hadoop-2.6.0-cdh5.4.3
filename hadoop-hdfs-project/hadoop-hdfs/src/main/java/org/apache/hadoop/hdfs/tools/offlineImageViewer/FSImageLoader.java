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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.protocol.proto.HdfsProtos;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatPBINode;
import org.apache.hadoop.hdfs.server.namenode.FSImageFormatProtobuf;
import org.apache.hadoop.hdfs.server.namenode.FSImageUtil;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.apache.hadoop.hdfs.server.namenode.INodeId;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.LimitInputStream;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * FSImageLoader loads fsimage and provide methods to return JSON formatted
 * file status of the namespace of the fsimage.
 */
class FSImageLoader {
  public static final Log LOG = LogFactory.getLog(FSImageHandler.class);

  private final String[] stringTable;
  // byte representation of inodes, sorted by id
  private final byte[][] inodes;
  private final Map<Long, long[]> dirmap;
  private static final Comparator<byte[]> INODE_BYTES_COMPARATOR = new
          Comparator<byte[]>() {
    @Override
    public int compare(byte[] o1, byte[] o2) {
      try {
        final FsImageProto.INodeSection.INode l = FsImageProto.INodeSection
                .INode.parseFrom(o1);
        final FsImageProto.INodeSection.INode r = FsImageProto.INodeSection
                .INode.parseFrom(o2);
        if (l.getId() < r.getId()) {
          return -1;
        } else if (l.getId() > r.getId()) {
          return 1;
        } else {
          return 0;
        }
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  };

  private FSImageLoader(String[] stringTable, byte[][] inodes,
                        Map<Long, long[]> dirmap) {
    this.stringTable = stringTable;
    this.inodes = inodes;
    this.dirmap = dirmap;
  }

  /**
   * Load fsimage into the memory.
   * @param inputFile the filepath of the fsimage to load.
   * @return FSImageLoader
   * @throws IOException if failed to load fsimage.
   */
  static FSImageLoader load(String inputFile) throws IOException {
    Configuration conf = new Configuration();
    RandomAccessFile file = new RandomAccessFile(inputFile, "r");
    if (!FSImageUtil.checkFileFormat(file)) {
      throw new IOException("Unrecognized FSImage");
    }

    FsImageProto.FileSummary summary = FSImageUtil.loadSummary(file);
    FileInputStream fin = null;

    try {
      // Map to record INodeReference to the referred id
      ImmutableList<Long> refIdList = null;
      String[] stringTable = null;
      byte[][] inodes = null;
      Map<Long, long[]> dirmap = null;

      fin = new FileInputStream(file.getFD());

      ArrayList<FsImageProto.FileSummary.Section> sections =
          Lists.newArrayList(summary.getSectionsList());
      Collections.sort(sections,
          new Comparator<FsImageProto.FileSummary.Section>() {
            @Override
            public int compare(FsImageProto.FileSummary.Section s1,
                               FsImageProto.FileSummary.Section s2) {
              FSImageFormatProtobuf.SectionName n1 =
                  FSImageFormatProtobuf.SectionName.fromString(s1.getName());
              FSImageFormatProtobuf.SectionName n2 =
                  FSImageFormatProtobuf.SectionName.fromString(s2.getName());
              if (n1 == null) {
                return n2 == null ? 0 : -1;
              } else if (n2 == null) {
                return -1;
              } else {
                return n1.ordinal() - n2.ordinal();
              }
            }
          });

      for (FsImageProto.FileSummary.Section s : sections) {
        fin.getChannel().position(s.getOffset());
        InputStream is = FSImageUtil.wrapInputStreamForCompression(conf,
            summary.getCodec(), new BufferedInputStream(new LimitInputStream(
            fin, s.getLength())));

        LOG.debug("Loading section " + s.getName() + " length: " + s.getLength
                ());
        switch (FSImageFormatProtobuf.SectionName.fromString(s.getName())) {
          case STRING_TABLE:
            stringTable = loadStringTable(is);
            break;
          case INODE:
            inodes = loadINodeSection(is);
            break;
          case INODE_REFERENCE:
            refIdList = loadINodeReferenceSection(is);
            break;
          case INODE_DIR:
            dirmap = loadINodeDirectorySection(is, refIdList);
            break;
          default:
            break;
        }
      }
      return new FSImageLoader(stringTable, inodes, dirmap);
    } finally {
      IOUtils.cleanup(null, fin);
    }
  }

  private static Map<Long, long[]> loadINodeDirectorySection
          (InputStream in, List<Long> refIdList)
      throws IOException {
    LOG.info("Loading inode directory section");
    Map<Long, long[]> dirs = Maps.newHashMap();
    long counter = 0;
    while (true) {
      FsImageProto.INodeDirectorySection.DirEntry e =
          FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
      // note that in is a LimitedInputStream
      if (e == null) {
        break;
      }
      ++counter;

      long[] l = new long[e.getChildrenCount() + e.getRefChildrenCount()];
      for (int i = 0; i < e.getChildrenCount(); ++i) {
        l[i] = e.getChildren(i);
      }
      for (int i = e.getChildrenCount(); i < l.length; i++) {
        int refId = e.getRefChildren(i - e.getChildrenCount());
        l[i] = refIdList.get(refId);
      }
      dirs.put(e.getParent(), l);
    }
    LOG.info("Loaded " + counter + " directories");
    return dirs;
  }

  private static ImmutableList<Long> loadINodeReferenceSection(InputStream in)
      throws IOException {
    LOG.info("Loading inode references");
    ImmutableList.Builder<Long> builder = ImmutableList.builder();
    long counter = 0;
    while (true) {
      FsImageProto.INodeReferenceSection.INodeReference e =
          FsImageProto.INodeReferenceSection.INodeReference
              .parseDelimitedFrom(in);
      if (e == null) {
        break;
      }
      ++counter;
      builder.add(e.getReferredId());
    }
    LOG.info("Loaded " + counter + " inode references");
    return builder.build();
  }

  private static byte[][] loadINodeSection(InputStream in)
          throws IOException {
    FsImageProto.INodeSection s = FsImageProto.INodeSection
        .parseDelimitedFrom(in);
    LOG.info("Loading " + s.getNumInodes() + " inodes.");
    final byte[][] inodes = new byte[(int) s.getNumInodes()][];

    for (int i = 0; i < s.getNumInodes(); ++i) {
      int size = CodedInputStream.readRawVarint32(in.read(), in);
      byte[] bytes = new byte[size];
      IOUtils.readFully(in, bytes, 0, size);
      inodes[i] = bytes;
    }
    LOG.debug("Sorting inodes");
    Arrays.sort(inodes, INODE_BYTES_COMPARATOR);
    LOG.debug("Finished sorting inodes");
    return inodes;
  }

  static String[] loadStringTable(InputStream in) throws
  IOException {
    FsImageProto.StringTableSection s = FsImageProto.StringTableSection
        .parseDelimitedFrom(in);
    LOG.info("Loading " + s.getNumEntry() + " strings");
    String[] stringTable = new String[s.getNumEntry() + 1];
    for (int i = 0; i < s.getNumEntry(); ++i) {
      FsImageProto.StringTableSection.Entry e = FsImageProto
          .StringTableSection.Entry.parseDelimitedFrom(in);
      stringTable[e.getId()] = e.getStr();
    }
    return stringTable;
  }

  /**
   * Return the JSON formatted FileStatus of the specified file.
   * @param path a path specifies a file
   * @return JSON formatted FileStatus
   * @throws IOException if failed to serialize fileStatus to JSON.
   */
  String getFileStatus(String path) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    FsImageProto.INodeSection.INode inode = fromINodeId(lookup(path));
    return "{\"FileStatus\":\n"
        + mapper.writeValueAsString(getFileStatus(inode, false)) + "\n}\n";
  }

  /**
   * Return the JSON formatted list of the files in the specified directory.
   * @param path a path specifies a directory to list
   * @return JSON formatted file list in the directory
   * @throws IOException if failed to serialize fileStatus to JSON.
   */
  String listStatus(String path) throws IOException {
    StringBuilder sb = new StringBuilder();
    ObjectMapper mapper = new ObjectMapper();
    List<Map<String, Object>> fileStatusList = getFileStatusList(path);
    sb.append("{\"FileStatuses\":{\"FileStatus\":[\n");
    int i = 0;
    for (Map<String, Object> fileStatusMap : fileStatusList) {
      if (i++ != 0) {
        sb.append(',');
      }
      sb.append(mapper.writeValueAsString(fileStatusMap));
    }
    sb.append("\n]}}\n");
    return sb.toString();
  }

  private List<Map<String, Object>> getFileStatusList(String path)
          throws IOException {
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    long id = lookup(path);
    FsImageProto.INodeSection.INode inode = fromINodeId(id);
    if (inode.getType() == FsImageProto.INodeSection.INode.Type.DIRECTORY) {
      if (!dirmap.containsKey(id)) {
        // if the directory is empty, return empty list
        return list;
      }
      long[] children = dirmap.get(id);
      for (long cid : children) {
        list.add(getFileStatus(fromINodeId(cid), true));
      }
    } else {
      list.add(getFileStatus(inode, false));
    }
    return list;
  }

  /**
   * Return the JSON formatted ACL status of the specified file.
   * @param path a path specifies a file
   * @return JSON formatted AclStatus
   * @throws IOException if failed to serialize fileStatus to JSON.
   */
  String getAclStatus(String path) throws IOException {
    StringBuilder sb = new StringBuilder();
    List<AclEntry> aclEntryList = getAclEntryList(path);
    PermissionStatus p = getPermissionStatus(path);
    sb.append("{\"AclStatus\":{\"entries\":[");
    int i = 0;
    for (AclEntry aclEntry : aclEntryList) {
      if (i++ != 0) {
        sb.append(',');
      }
      sb.append('"');
      sb.append(aclEntry.toString());
      sb.append('"');
    }
    sb.append("],\"group\": \"");
    sb.append(p.getGroupName());
    sb.append("\",\"owner\": \"");
    sb.append(p.getUserName());
    sb.append("\",\"stickyBit\": ");
    sb.append(p.getPermission().getStickyBit());
    sb.append("}}\n");
    return sb.toString();
  }

  private List<AclEntry> getAclEntryList(String path) throws IOException {
    long id = lookup(path);
    FsImageProto.INodeSection.INode inode = fromINodeId(id);
    switch (inode.getType()) {
      case FILE: {
        FsImageProto.INodeSection.INodeFile f = inode.getFile();
        return FSImageFormatPBINode.Loader.loadAclEntries(
            f.getAcl(), stringTable);
      }
      case DIRECTORY: {
        FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
        return FSImageFormatPBINode.Loader.loadAclEntries(
            d.getAcl(), stringTable);
      }
      default: {
        return new ArrayList<AclEntry>();
      }
    }
  }

  private PermissionStatus getPermissionStatus(String path) throws IOException {
    long id = lookup(path);
    FsImageProto.INodeSection.INode inode = fromINodeId(id);
    switch (inode.getType()) {
      case FILE: {
        FsImageProto.INodeSection.INodeFile f = inode.getFile();
        return FSImageFormatPBINode.Loader.loadPermission(
            f.getPermission(), stringTable);
      }
      case DIRECTORY: {
        FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
        return FSImageFormatPBINode.Loader.loadPermission(
            d.getPermission(), stringTable);
      }
      case SYMLINK: {
        FsImageProto.INodeSection.INodeSymlink s = inode.getSymlink();
        return FSImageFormatPBINode.Loader.loadPermission(
            s.getPermission(), stringTable);
      }
      default: {
        return null;
      }
    }
  }

  /**
   * Return the INodeId of the specified path.
   */
  private long lookup(String path) throws IOException {
    Preconditions.checkArgument(path.startsWith("/"));
    long id = INodeId.ROOT_INODE_ID;
    for (int offset = 0, next; offset < path.length(); offset = next) {
      next = path.indexOf('/', offset + 1);
      if (next == -1) {
        next = path.length();
      }
      if (offset + 1 > next) {
        break;
      }

      final String component = path.substring(offset + 1, next);

      if (component.isEmpty()) {
        continue;
      }

      final long[] children = dirmap.get(id);
      if (children == null) {
        throw new FileNotFoundException(path);
      }

      boolean found = false;
      for (long cid : children) {
        FsImageProto.INodeSection.INode child = fromINodeId(cid);
        if (component.equals(child.getName().toStringUtf8())) {
          found = true;
          id = child.getId();
          break;
        }
      }
      if (!found) {
        throw new FileNotFoundException(path);
      }
    }
    return id;
  }

  private Map<String, Object> getFileStatus
      (FsImageProto.INodeSection.INode inode, boolean printSuffix){
    Map<String, Object> map = Maps.newHashMap();
    switch (inode.getType()) {
      case FILE: {
        FsImageProto.INodeSection.INodeFile f = inode.getFile();
        PermissionStatus p = FSImageFormatPBINode.Loader.loadPermission(
            f.getPermission(), stringTable);
        map.put("accessTime", f.getAccessTime());
        map.put("blockSize", f.getPreferredBlockSize());
        map.put("group", p.getGroupName());
        map.put("length", getFileSize(f));
        map.put("modificationTime", f.getModificationTime());
        map.put("owner", p.getUserName());
        map.put("pathSuffix",
            printSuffix ? inode.getName().toStringUtf8() : "");
        map.put("permission", toString(p.getPermission()));
        map.put("replication", f.getReplication());
        map.put("type", inode.getType());
        map.put("fileId", inode.getId());
        map.put("childrenNum", 0);
        return map;
      }
      case DIRECTORY: {
        FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
        PermissionStatus p = FSImageFormatPBINode.Loader.loadPermission(
            d.getPermission(), stringTable);
        map.put("accessTime", 0);
        map.put("blockSize", 0);
        map.put("group", p.getGroupName());
        map.put("length", 0);
        map.put("modificationTime", d.getModificationTime());
        map.put("owner", p.getUserName());
        map.put("pathSuffix",
            printSuffix ? inode.getName().toStringUtf8() : "");
        map.put("permission", toString(p.getPermission()));
        map.put("replication", 0);
        map.put("type", inode.getType());
        map.put("fileId", inode.getId());
        map.put("childrenNum", dirmap.containsKey(inode.getId()) ?
            dirmap.get(inode.getId()).length : 0);
        return map;
      }
      case SYMLINK: {
        FsImageProto.INodeSection.INodeSymlink d = inode.getSymlink();
        PermissionStatus p = FSImageFormatPBINode.Loader.loadPermission(
            d.getPermission(), stringTable);
        map.put("accessTime", d.getAccessTime());
        map.put("blockSize", 0);
        map.put("group", p.getGroupName());
        map.put("length", 0);
        map.put("modificationTime", d.getModificationTime());
        map.put("owner", p.getUserName());
        map.put("pathSuffix",
            printSuffix ? inode.getName().toStringUtf8() : "");
        map.put("permission", toString(p.getPermission()));
        map.put("replication", 0);
        map.put("type", inode.getType());
        map.put("symlink", d.getTarget().toStringUtf8());
        map.put("fileId", inode.getId());
        map.put("childrenNum", 0);
        return map;
      }
      default:
        return null;
    }
  }

  static long getFileSize(FsImageProto.INodeSection.INodeFile f) {
    long size = 0;
    for (HdfsProtos.BlockProto p : f.getBlocksList()) {
      size += p.getNumBytes();
    }
    return size;
  }

  private String toString(FsPermission permission) {
    return String.format("%o", permission.toShort());
  }

  private FsImageProto.INodeSection.INode fromINodeId(final long id)
          throws IOException {
    int l = 0, r = inodes.length;
    while (l < r) {
      int mid = l + (r - l) / 2;
      FsImageProto.INodeSection.INode n = FsImageProto.INodeSection.INode
              .parseFrom(inodes[mid]);
      long nid = n.getId();
      if (id > nid) {
        l = mid + 1;
      } else if (id < nid) {
        r = mid;
      } else {
        return n;
      }
    }
    return null;
  }
}
