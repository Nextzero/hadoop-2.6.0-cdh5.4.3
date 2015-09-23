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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.MemoryRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class TestQueueParsing {

  private static final Log LOG = LogFactory.getLog(TestQueueParsing.class);
  
  private static final double DELTA = 0.000001;
  
  private RMNodeLabelsManager nodeLabelManager;
  
  @Before
  public void setup() {
    nodeLabelManager = new MemoryRMNodeLabelsManager();
    nodeLabelManager.init(new YarnConfiguration());
    nodeLabelManager.start();
  }
  
  @Test
  public void testQueueParsing() throws Exception {
    CapacitySchedulerConfiguration csConf = 
      new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    capacityScheduler.setConf(conf);
    capacityScheduler.setRMContext(TestUtils.getMockRMContext());
    capacityScheduler.init(conf);
    capacityScheduler.start();
    capacityScheduler.reinitialize(conf, TestUtils.getMockRMContext());
    
    CSQueue a = capacityScheduler.getQueue("a");
    Assert.assertEquals(0.10, a.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a.getAbsoluteMaximumCapacity(), DELTA);
    
    CSQueue b1 = capacityScheduler.getQueue("b1");
    Assert.assertEquals(0.2 * 0.5, b1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Parent B has no MAX_CAP", 
        0.85, b1.getAbsoluteMaximumCapacity(), DELTA);
    
    CSQueue c12 = capacityScheduler.getQueue("c12");
    Assert.assertEquals(0.7 * 0.5 * 0.45, c12.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.7 * 0.55 * 0.7, 
        c12.getAbsoluteMaximumCapacity(), DELTA);
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf) {
    
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 20);
    
    final String C = CapacitySchedulerConfiguration.ROOT + ".c";
    conf.setCapacity(C, 70);
    conf.setMaximumCapacity(C, 70);

    LOG.info("Setup top-level queues");
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 45);
    conf.setCapacity(A2, 70);
    conf.setMaximumCapacity(A2, 85);
    
    final String B1 = B + ".b1";
    final String B2 = B + ".b2";
    final String B3 = B + ".b3";
    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setCapacity(B1, 50);
    conf.setMaximumCapacity(B1, 85);
    conf.setCapacity(B2, 30);
    conf.setMaximumCapacity(B2, 35);
    conf.setCapacity(B3, 20);
    conf.setMaximumCapacity(B3, 35);

    final String C1 = C + ".c1";
    final String C2 = C + ".c2";
    final String C3 = C + ".c3";
    final String C4 = C + ".c4";
    conf.setQueues(C, new String[] {"c1", "c2", "c3", "c4"});
    conf.setCapacity(C1, 50);
    conf.setMaximumCapacity(C1, 55);
    conf.setCapacity(C2, 10);
    conf.setMaximumCapacity(C2, 25);
    conf.setCapacity(C3, 35);
    conf.setMaximumCapacity(C3, 38);
    conf.setCapacity(C4, 5);
    conf.setMaximumCapacity(C4, 5);
    
    LOG.info("Setup 2nd-level queues");
    
    // Define 3rd-level queues
    final String C11 = C1 + ".c11";
    final String C12 = C1 + ".c12";
    final String C13 = C1 + ".c13";
    conf.setQueues(C1, new String[] {"c11", "c12", "c13"});
    conf.setCapacity(C11, 15);
    conf.setMaximumCapacity(C11, 30);
    conf.setCapacity(C12, 45);
    conf.setMaximumCapacity(C12, 70);
    conf.setCapacity(C13, 40);
    conf.setMaximumCapacity(C13, 40);
    
    LOG.info("Setup 3rd-level queues");
  }

  @Test (expected=java.lang.IllegalArgumentException.class)
  public void testRootQueueParsing() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();

    // non-100 percent value will throw IllegalArgumentException
    conf.setCapacity(CapacitySchedulerConfiguration.ROOT, 90);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    capacityScheduler.setConf(new YarnConfiguration());
    capacityScheduler.init(conf);
    capacityScheduler.start();
    capacityScheduler.reinitialize(conf, null);
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  public void testMaxCapacity() throws Exception {
    CapacitySchedulerConfiguration conf = new CapacitySchedulerConfiguration();

    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b", "c"});

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 50);
    conf.setMaximumCapacity(A, 60);

    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 50);
    conf.setMaximumCapacity(B, 45);  // Should throw an exception


    boolean fail = false;
    CapacityScheduler capacityScheduler;
    try {
      capacityScheduler = new CapacityScheduler();
      capacityScheduler.setConf(new YarnConfiguration());
      capacityScheduler.init(conf);
      capacityScheduler.start();
      capacityScheduler.reinitialize(conf, null);
    } catch (IllegalArgumentException iae) {
      fail = true;
    }
    Assert.assertTrue("Didn't throw IllegalArgumentException for wrong maxCap", 
        fail);

    conf.setMaximumCapacity(B, 60);
    
    // Now this should work
    capacityScheduler = new CapacityScheduler();
    capacityScheduler.setConf(new YarnConfiguration());
    capacityScheduler.init(conf);
    capacityScheduler.start();
    capacityScheduler.reinitialize(conf, null);
    
    fail = false;
    try {
    LeafQueue a = (LeafQueue)capacityScheduler.getQueue(A);
    a.setMaxCapacity(45);
    } catch  (IllegalArgumentException iae) {
      fail = true;
    }
    Assert.assertTrue("Didn't throw IllegalArgumentException for wrong " +
    		"setMaxCap", fail);
    capacityScheduler.stop();
  }
  
  private void setupQueueConfigurationWithoutLabels(CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 90);

    LOG.info("Setup top-level queues");
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 45);
    conf.setCapacity(A2, 70);
    conf.setMaximumCapacity(A2, 85);
    
    final String B1 = B + ".b1";
    final String B2 = B + ".b2";
    final String B3 = B + ".b3";
    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setCapacity(B1, 50);
    conf.setMaximumCapacity(B1, 85);
    conf.setCapacity(B2, 30);
    conf.setMaximumCapacity(B2, 35);
    conf.setCapacity(B3, 20);
    conf.setMaximumCapacity(B3, 35);
  }
  
  private void setupQueueConfigurationWithLabels(CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "red", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "blue", 100);

    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 90);

    LOG.info("Setup top-level queues");
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(A, "red", 50);
    conf.setMaximumCapacityByLabel(A, "red", 50);
    conf.setCapacityByLabel(A, "blue", 50);
    
    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 45);
    conf.setCapacityByLabel(A1, "red", 50);
    conf.setCapacityByLabel(A1, "blue", 100);
    
    conf.setCapacity(A2, 70);
    conf.setMaximumCapacity(A2, 85);
    conf.setAccessibleNodeLabels(A2, ImmutableSet.of("red"));
    conf.setCapacityByLabel(A2, "red", 50);
    conf.setMaximumCapacityByLabel(A2, "red", 60);
    
    final String B1 = B + ".b1";
    final String B2 = B + ".b2";
    final String B3 = B + ".b3";
    conf.setQueues(B, new String[] {"b1", "b2", "b3"});
    conf.setAccessibleNodeLabels(B, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(B, "red", 50);
    conf.setCapacityByLabel(B, "blue", 50);
    
    conf.setCapacity(B1, 50);
    conf.setMaximumCapacity(B1, 85);
    conf.setCapacityByLabel(B1, "red", 50);
    conf.setCapacityByLabel(B1, "blue", 50);
    
    conf.setCapacity(B2, 30);
    conf.setMaximumCapacity(B2, 35);
    conf.setCapacityByLabel(B2, "red", 25);
    conf.setCapacityByLabel(B2, "blue", 25);
    
    conf.setCapacity(B3, 20);
    conf.setMaximumCapacity(B3, 35);
    conf.setCapacityByLabel(B3, "red", 25);
    conf.setCapacityByLabel(B3, "blue", 25);
  }
  
  private void setupQueueConfigurationWithLabelsInherit(
      CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "red", 100);
    conf.setCapacityByLabel(CapacitySchedulerConfiguration.ROOT, "blue", 100);

    // Set A configuration
    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setQueues(A, new String[] {"a1", "a2"});
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(A, "red", 100);
    conf.setCapacityByLabel(A, "blue", 100);
    
    // Set B configuraiton
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 90);
    conf.setAccessibleNodeLabels(B, CommonNodeLabelsManager.EMPTY_STRING_SET);
    
    // Define 2nd-level queues
    final String A1 = A + ".a1";
    final String A2 = A + ".a2";
    
    conf.setCapacity(A1, 30);
    conf.setMaximumCapacity(A1, 45);
    conf.setCapacityByLabel(A1, "red", 50);
    conf.setCapacityByLabel(A1, "blue", 100);
    
    conf.setCapacity(A2, 70);
    conf.setMaximumCapacity(A2, 85);
    conf.setAccessibleNodeLabels(A2, ImmutableSet.of("red"));
    conf.setCapacityByLabel(A2, "red", 50);
  }
  
  private void setupQueueConfigurationWithSingleLevel(
      CapacitySchedulerConfiguration conf) {
    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT, new String[] {"a", "b"});

    // Set A configuration
    final String A = CapacitySchedulerConfiguration.ROOT + ".a";
    conf.setCapacity(A, 10);
    conf.setMaximumCapacity(A, 15);
    conf.setAccessibleNodeLabels(A, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(A, "red", 90);
    conf.setCapacityByLabel(A, "blue", 90);
    
    // Set B configuraiton
    final String B = CapacitySchedulerConfiguration.ROOT + ".b";
    conf.setCapacity(B, 90);
    conf.setAccessibleNodeLabels(B, ImmutableSet.of("red", "blue"));
    conf.setCapacityByLabel(B, "red", 10);
    conf.setCapacityByLabel(B, "blue", 10);
  }
  
  @Test
  public void testQueueParsingReinitializeWithLabels() throws IOException {
    nodeLabelManager.addToCluserNodeLabels(ImmutableSet.of("red", "blue"));
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithoutLabels(csConf);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(conf),
            new NMTokenSecretManagerInRM(conf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(conf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(conf);
    capacityScheduler.start();
    csConf = new CapacitySchedulerConfiguration();
    setupQueueConfigurationWithLabels(csConf);
    conf = new YarnConfiguration(csConf);
    capacityScheduler.reinitialize(conf, rmContext);
    checkQueueLabels(capacityScheduler);
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  private void checkQueueLabels(CapacityScheduler capacityScheduler) {
    // queue-A is red, blue
    Assert.assertTrue(capacityScheduler.getQueue("a").getAccessibleNodeLabels()
        .containsAll(ImmutableSet.of("red", "blue")));

    // queue-A1 inherits A's configuration
    Assert.assertTrue(capacityScheduler.getQueue("a1")
        .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));

    // queue-A2 is "red"
    Assert.assertEquals(1, capacityScheduler.getQueue("a2")
        .getAccessibleNodeLabels().size());
    Assert.assertTrue(capacityScheduler.getQueue("a2")
        .getAccessibleNodeLabels().contains("red"));

    // queue-B is "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b").getAccessibleNodeLabels()
        .containsAll(ImmutableSet.of("red", "blue")));

    // queue-B2 inherits "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b2")
        .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));
    
    // check capacity of A2
    CSQueue qA2 = capacityScheduler.getQueue("a2");
    Assert.assertEquals(0.7, qA2.getCapacity(), DELTA);
    Assert.assertEquals(0.5, qA2.getCapacityByNodeLabel("red"), DELTA);
    Assert.assertEquals(0.07, qA2.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.25, qA2.getAbsoluteCapacityByNodeLabel("red"), DELTA);
    Assert.assertEquals(0.1275, qA2.getAbsoluteMaximumCapacity(), DELTA);
    Assert.assertEquals(0.3, qA2.getAbsoluteMaximumCapacityByNodeLabel("red"), DELTA);
    
    // check capacity of B3
    CSQueue qB3 = capacityScheduler.getQueue("b3");
    Assert.assertEquals(0.18, qB3.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.125, qB3.getAbsoluteCapacityByNodeLabel("red"), DELTA);
    Assert.assertEquals(0.35, qB3.getAbsoluteMaximumCapacity(), DELTA);
    Assert.assertEquals(1, qB3.getAbsoluteMaximumCapacityByNodeLabel("red"), DELTA);
  }
  
  private void
      checkQueueLabelsInheritConfig(CapacityScheduler capacityScheduler) {
    // queue-A is red, blue
    Assert.assertTrue(capacityScheduler.getQueue("a").getAccessibleNodeLabels()
        .containsAll(ImmutableSet.of("red", "blue")));

    // queue-A1 inherits A's configuration
    Assert.assertTrue(capacityScheduler.getQueue("a1")
        .getAccessibleNodeLabels().containsAll(ImmutableSet.of("red", "blue")));

    // queue-A2 is "red"
    Assert.assertEquals(1, capacityScheduler.getQueue("a2")
        .getAccessibleNodeLabels().size());
    Assert.assertTrue(capacityScheduler.getQueue("a2")
        .getAccessibleNodeLabels().contains("red"));

    // queue-B is "red"/"blue"
    Assert.assertTrue(capacityScheduler.getQueue("b").getAccessibleNodeLabels()
        .isEmpty());
  }
  
  @Test
  public void testQueueParsingWithLabels() throws IOException {
    nodeLabelManager.addToCluserNodeLabels(ImmutableSet.of("red", "blue"));
    
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabels(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    checkQueueLabels(capacityScheduler);
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  @Test
  public void testQueueParsingWithLabelsInherit() throws IOException {
    nodeLabelManager.addToCluserNodeLabels(ImmutableSet.of("red", "blue"));

    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabelsInherit(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    checkQueueLabelsInheritConfig(capacityScheduler);
    ServiceOperations.stopQuietly(capacityScheduler);
  }
  
  @Test(expected = Exception.class)
  public void testQueueParsingWhenLabelsNotExistedInNodeLabelManager()
      throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabels(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    
    RMNodeLabelsManager nodeLabelsManager = new MemoryRMNodeLabelsManager();
    nodeLabelsManager.init(conf);
    nodeLabelsManager.start();
    
    rmContext.setNodeLabelManager(nodeLabelsManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
    ServiceOperations.stopQuietly(nodeLabelsManager);
  }
  
  @Test(expected = Exception.class)
  public void testQueueParsingWhenLabelsInheritedNotExistedInNodeLabelManager()
      throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabelsInherit(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    
    RMNodeLabelsManager nodeLabelsManager = new MemoryRMNodeLabelsManager();
    nodeLabelsManager.init(conf);
    nodeLabelsManager.start();
    
    rmContext.setNodeLabelManager(nodeLabelsManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
    ServiceOperations.stopQuietly(nodeLabelsManager);
  }
  
  @Test(expected = Exception.class)
  public void testSingleLevelQueueParsingWhenLabelsNotExistedInNodeLabelManager()
      throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithSingleLevel(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    
    RMNodeLabelsManager nodeLabelsManager = new MemoryRMNodeLabelsManager();
    nodeLabelsManager.init(conf);
    nodeLabelsManager.start();
    
    rmContext.setNodeLabelManager(nodeLabelsManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
    ServiceOperations.stopQuietly(nodeLabelsManager);
  }
  
  @Test(expected = Exception.class)
  public void testQueueParsingWhenLabelsNotExist() throws IOException {
    YarnConfiguration conf = new YarnConfiguration();
    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration(conf);
    setupQueueConfigurationWithLabels(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    
    RMNodeLabelsManager nodeLabelsManager = new MemoryRMNodeLabelsManager();
    nodeLabelsManager.init(conf);
    nodeLabelsManager.start();
    
    rmContext.setNodeLabelManager(nodeLabelsManager);
    capacityScheduler.setConf(csConf);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(csConf);
    capacityScheduler.start();
    ServiceOperations.stopQuietly(capacityScheduler);
    ServiceOperations.stopQuietly(nodeLabelsManager);
  }
  
  @Test
  public void testQueueParsingWithUnusedLabels() throws IOException {
    final ImmutableSet<String> labels = ImmutableSet.of("red", "blue");
    
    // Initialize a cluster with labels, but doesn't use them, reinitialize
    // shouldn't fail
    nodeLabelManager.addToCluserNodeLabels(labels);

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    setupQueueConfiguration(csConf);
    csConf.setAccessibleNodeLabels(CapacitySchedulerConfiguration.ROOT, labels);
    YarnConfiguration conf = new YarnConfiguration(csConf);

    CapacityScheduler capacityScheduler = new CapacityScheduler();
    capacityScheduler.setConf(conf);
    RMContextImpl rmContext =
        new RMContextImpl(null, null, null, null, null, null,
            new RMContainerTokenSecretManager(csConf),
            new NMTokenSecretManagerInRM(csConf),
            new ClientToAMTokenSecretManagerInRM(), null);
    rmContext.setNodeLabelManager(nodeLabelManager);
    capacityScheduler.setRMContext(rmContext);
    capacityScheduler.init(conf);
    capacityScheduler.start();
    capacityScheduler.reinitialize(conf, rmContext);
    
    // check root queue's capacity by label -- they should be all zero
    CSQueue root = capacityScheduler.getQueue(CapacitySchedulerConfiguration.ROOT);
    Assert.assertEquals(0, root.getCapacityByNodeLabel("red"), DELTA);
    Assert.assertEquals(0, root.getCapacityByNodeLabel("blue"), DELTA);

    CSQueue a = capacityScheduler.getQueue("a");
    Assert.assertEquals(0.10, a.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.15, a.getAbsoluteMaximumCapacity(), DELTA);

    CSQueue b1 = capacityScheduler.getQueue("b1");
    Assert.assertEquals(0.2 * 0.5, b1.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals("Parent B has no MAX_CAP", 0.85,
        b1.getAbsoluteMaximumCapacity(), DELTA);

    CSQueue c12 = capacityScheduler.getQueue("c12");
    Assert.assertEquals(0.7 * 0.5 * 0.45, c12.getAbsoluteCapacity(), DELTA);
    Assert.assertEquals(0.7 * 0.55 * 0.7, c12.getAbsoluteMaximumCapacity(),
        DELTA);
    capacityScheduler.stop();
  }
}
