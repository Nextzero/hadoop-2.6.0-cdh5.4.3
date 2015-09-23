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

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

public class TestJvmContext {

  @Test
  public void testJVMIdWriteLongReadLong() throws IOException {
    JvmContext jvmContext = new JvmContext(
        new JVMId("jt-1", 1, true, 1L + Integer.MAX_VALUE),
        JvmContext.HARDCODED_PID
    );

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    jvmContext.write(out);

    DataInput in =
        new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    jvmContext.readFields(in);
    Assert.assertEquals("PIDs don't match", JvmContext.HARDCODED_PID,
        jvmContext.pid);
  }

  @Test
  public void testJVMIdWriteIntReadInt() throws IOException {
    JvmContext jvmContext = new JvmContext(
        new JVMId("jt-1", 1, true, 1),
        JvmContext.HARDCODED_PID
    );

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    jvmContext.write(out);

    DataInput in =
        new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    jvmContext.readFields(in);
    Assert.assertEquals("PIDs don't match", JvmContext.HARDCODED_PID,
        jvmContext.pid);
  }

  @Test (expected = RuntimeException.class)
  public void testHardcodedPid() throws IOException {
    JvmContext jvmContext = new JvmContext(
        new JVMId("jt-1", 1, true, 1),
        "1"
    );

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    jvmContext.write(out);

    DataInput in =
        new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
    jvmContext.readFields(in);
  }
}
