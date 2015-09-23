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

import junit.framework.TestCase;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Assert;

/**
 * Test {@link JobTracker} w.r.t resolveAndAddToTopology.
 */
public class TestJobTrackerTopology extends TestCase {

  public void testJobTrackerResolveAndAddToTopology() throws Exception {
    JobConf conf = new JobConf();
    conf.set(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY,
        "test.sh");
    conf = MiniMRCluster.configureJobConf(conf, "file:///", 0, 0, null);

    JobTracker jt = JobTracker.startTracker(conf);
    try {
      jt.resolveAndAddToTopology("test.host");
    } catch (NullPointerException e) {
      Assert.fail("NullPointerException should not happen");
    }
    jt.stopTracker();
  }
}