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

package org.apache.hadoop.mapred;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

class JvmContext implements Writable {

  public static final Log LOG =
    LogFactory.getLog(JvmContext.class);

  // Tasks hardcode the pid to -1000 when calling TaskUmbilicalProtocol#getTask
  static final String HARDCODED_PID = "-1000";
  
  JVMId jvmId;
  String pid;
  
  JvmContext() {
    jvmId = new JVMId();
    pid = "";
  }
  
  JvmContext(JVMId id, String pid) {
    jvmId = id;
    this.pid = pid;
  }

  /**
   * Helper method that reads inBytes to construct JvmContext.
   *
   * @param inBytes input bytes
   * @param longJvmId if JVMId should be considered long
   * @return a valid JvmContext on a successful parse, null otherwise
   * @throws IOException
   */
  private static JvmContext readFieldsInternal(
      byte[] inBytes, boolean longJvmId) throws IOException {
    DataInput in = new DataInputStream(new ByteArrayInputStream(inBytes));
    JVMId jvmId = new JVMId();

    try {
      if (longJvmId) {
        jvmId.readFieldsJvmIdAsLong(in);
      } else {
        jvmId.readFieldsJvmIdAsInt(in);
      }
    } catch (Exception e) {
      return null;
    }

    return new JvmContext(jvmId, Text.readString(in));
  }

  /**
   * Works with both 5.3.x and 5.4.x even though they use int and long
   * respectively for the id in JVMId.
   */
  public void readFields(DataInput in) throws IOException {
    // "in" is essentially backed by a byte[], per {@link Server#processOneRpc}
    DataInputStream dis = (DataInputStream) in;
    int inLen = dis.available();
    byte[] inBytes = new byte[inLen];
    in.readFully(inBytes);
    assert(dis.available() == 0);

    // Try reading JvmContext assuming JVMId uses long for id.
    JvmContext jvmContext = readFieldsInternal(inBytes, true);
    if (LOG.isDebugEnabled()) {
      LOG.debug("JVMContext read interpreting JVMId as long: "
          + jvmContext.jvmId + "  " + jvmContext.pid);
    }

    if (jvmContext == null || !jvmContext.pid.equals(HARDCODED_PID)) {
      // Looks like reading it as long didn't work. Fall back to reading as int.
      jvmContext = readFieldsInternal(inBytes, false);
      if (LOG.isDebugEnabled()) {
        LOG.debug("JVMContext read interpreting JVMId as int: "
            + jvmContext.jvmId + "  " + jvmContext.pid);
      }

      if (jvmContext == null || !jvmContext.pid.equals(HARDCODED_PID)) {
        // Reading as int didn't work either. Give up!
        throw new RuntimeException("Invalid JVMContext received!");
      }
    }

    this.jvmId = jvmContext.jvmId;
    this.pid = jvmContext.pid;
  }
  
  public void write(DataOutput out) throws IOException {
    jvmId.write(out);
    Text.writeString(out, pid);
  }
}
