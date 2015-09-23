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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.ahs.RMApplicationHistoryWriter;
import org.apache.hadoop.yarn.server.resourcemanager.metrics.SystemMetricsPublisher;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.ContainerAllocationExpirer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestReservations {

  private static final Log LOG = LogFactory.getLog(TestReservations.class);

  private final RecordFactory recordFactory = RecordFactoryProvider
      .getRecordFactory(null);

  RMContext rmContext;
  CapacityScheduler cs;
  // CapacitySchedulerConfiguration csConf;
  CapacitySchedulerContext csContext;

  private final ResourceCalculator resourceCalculator = new DefaultResourceCalculator();

  CSQueue root;
  Map<String, CSQueue> queues = new HashMap<String, CSQueue>();
  Map<String, CSQueue> oldQueues = new HashMap<String, CSQueue>();

  final static int GB = 1024;
  final static String DEFAULT_RACK = "/default";

  @Before
  public void setUp() throws Exception {
    CapacityScheduler spyCs = new CapacityScheduler();
    cs = spy(spyCs);
    rmContext = TestUtils.getMockRMContext();

  }

  private void setup(CapacitySchedulerConfiguration csConf) throws Exception {

    csConf.setBoolean("yarn.scheduler.capacity.user-metrics.enable", true);
    final String newRoot = "root" + System.currentTimeMillis();
    // final String newRoot = "root";

    setupQueueConfiguration(csConf, newRoot);
    YarnConfiguration conf = new YarnConfiguration();
    cs.setConf(conf);

    csContext = mock(CapacitySchedulerContext.class);
    when(csContext.getConfiguration()).thenReturn(csConf);
    when(csContext.getConf()).thenReturn(conf);
    when(csContext.getMinimumResourceCapability()).thenReturn(
        Resources.createResource(GB, 1));
    when(csContext.getMaximumResourceCapability()).thenReturn(
        Resources.createResource(16 * GB, 12));
    when(csContext.getClusterResource()).thenReturn(
        Resources.createResource(100 * 16 * GB, 100 * 12));
    when(csContext.getApplicationComparator()).thenReturn(
        CapacityScheduler.applicationComparator);
    when(csContext.getQueueComparator()).thenReturn(
        CapacityScheduler.queueComparator);
    when(csContext.getResourceCalculator()).thenReturn(resourceCalculator);
    when(csContext.getRMContext()).thenReturn(rmContext);
    RMContainerTokenSecretManager containerTokenSecretManager = new RMContainerTokenSecretManager(
        conf);
    containerTokenSecretManager.rollMasterKey();
    when(csContext.getContainerTokenSecretManager()).thenReturn(
        containerTokenSecretManager);

    root = CapacityScheduler.parseQueue(csContext, csConf, null,
        CapacitySchedulerConfiguration.ROOT, queues, queues, TestUtils.spyHook);

    cs.setRMContext(rmContext);
    cs.init(csConf);
    cs.start();
  }

  private static final String A = "a";

  private void setupQueueConfiguration(CapacitySchedulerConfiguration conf,
      final String newRoot) {

    // Define top-level queues
    conf.setQueues(CapacitySchedulerConfiguration.ROOT,
        new String[] { newRoot });
    conf.setMaximumCapacity(CapacitySchedulerConfiguration.ROOT, 100);
    conf.setAcl(CapacitySchedulerConfiguration.ROOT,
        QueueACL.SUBMIT_APPLICATIONS, " ");

    final String Q_newRoot = CapacitySchedulerConfiguration.ROOT + "."
        + newRoot;
    conf.setQueues(Q_newRoot, new String[] { A });
    conf.setCapacity(Q_newRoot, 100);
    conf.setMaximumCapacity(Q_newRoot, 100);
    conf.setAcl(Q_newRoot, QueueACL.SUBMIT_APPLICATIONS, " ");

    final String Q_A = Q_newRoot + "." + A;
    conf.setCapacity(Q_A, 100f);
    conf.setMaximumCapacity(Q_A, 100);
    conf.setAcl(Q_A, QueueACL.SUBMIT_APPLICATIONS, "*");

  }

  static LeafQueue stubLeafQueue(LeafQueue queue) {

    // Mock some methods for ease in these unit tests

    // 1. LeafQueue.createContainer to return dummy containers
    doAnswer(new Answer<Container>() {
      @Override
      public Container answer(InvocationOnMock invocation) throws Throwable {
        final FiCaSchedulerApp application = (FiCaSchedulerApp) (invocation
            .getArguments()[0]);
        final ContainerId containerId = TestUtils
            .getMockContainerId(application);

        Container container = TestUtils.getMockContainer(containerId,
            ((FiCaSchedulerNode) (invocation.getArguments()[1])).getNodeID(),
            (Resource) (invocation.getArguments()[2]),
            ((Priority) invocation.getArguments()[3]));
        return container;
      }
    }).when(queue).createContainer(any(FiCaSchedulerApp.class),
        any(FiCaSchedulerNode.class), any(Resource.class), any(Priority.class));

    // 2. Stub out LeafQueue.parent.completedContainer
    CSQueue parent = queue.getParent();
    doNothing().when(parent).completedContainer(any(Resource.class),
        any(FiCaSchedulerApp.class), any(FiCaSchedulerNode.class),
        any(RMContainer.class), any(ContainerStatus.class),
        any(RMContainerEventType.class), any(CSQueue.class), anyBoolean());

    return queue;
  }

  @Test
  public void testReservation() throws Exception {
    // Test that we now unreserve and use a node that has space

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), rmContext);

    a.submitApplicationAttempt(app_0, user_0); 

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), rmContext);
    a.submitApplicationAttempt(app_1, user_0); 

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);
    String host_2 = "host_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, DEFAULT_RACK, 0,
        8 * GB);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getNode(node_2.getNodeID())).thenReturn(node_2);

    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 5 * GB, 2, true,
            priorityReduce, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 3 * GB, 2, true,
            priorityMap, recordFactory)));

    // Start testing...
    // Only AM
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(22 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getUsedResource().getMemory());
    assertEquals(0 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());

    // Only 1 map - simulating reduce
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(5 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(19 * GB, a.getMetrics().getAvailableMB());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(0 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());

    // Only 1 map to other node - simulating reduce
    a.assignContainers(clusterResource, node_1, false);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(16 * GB, a.getMetrics().getAvailableMB());
    assertEquals(16 * GB, app_0.getHeadroom().getMemory());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());
    assertEquals(2, app_0.getTotalRequiredResources(priorityReduce));

    // try to assign reducer (5G on node 0 and should reserve)
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(11 * GB, a.getMetrics().getAvailableMB());
    assertEquals(11 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getReservedContainer().getReservedResource()
        .getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());
    assertEquals(2, app_0.getTotalRequiredResources(priorityReduce));

    // assign reducer to node 2
    a.assignContainers(clusterResource, node_2, false);
    assertEquals(18 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(6 * GB, a.getMetrics().getAvailableMB());
    assertEquals(6 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getReservedContainer().getReservedResource()
        .getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(5 * GB, node_2.getUsedResource().getMemory());
    assertEquals(1, app_0.getTotalRequiredResources(priorityReduce));

    // node_1 heartbeat and unreserves from node_0 in order to allocate
    // on node_1
    a.assignContainers(clusterResource, node_1, false);
    assertEquals(18 * GB, a.getUsedResources().getMemory());
    assertEquals(18 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(18 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(6 * GB, a.getMetrics().getAvailableMB());
    assertEquals(6 * GB, app_0.getHeadroom().getMemory());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(8 * GB, node_1.getUsedResource().getMemory());
    assertEquals(5 * GB, node_2.getUsedResource().getMemory());
    assertEquals(0, app_0.getTotalRequiredResources(priorityReduce));
  }

  @Test
  public void testReservationNoContinueLook() throws Exception {
    // Test that with reservations-continue-look-all-nodes feature off
    // we don't unreserve and show we could get stuck

    queues = new HashMap<String, CSQueue>();
    // test that the deadlock occurs when turned off
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    csConf.setBoolean(
        "yarn.scheduler.capacity.reservations-continue-look-all-nodes", false);
    setup(csConf);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), rmContext);

    a.submitApplicationAttempt(app_0, user_0); 

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), rmContext);
    a.submitApplicationAttempt(app_1, user_0); 

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);
    String host_2 = "host_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, DEFAULT_RACK, 0,
        8 * GB);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getNode(node_2.getNodeID())).thenReturn(node_2);

    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 5 * GB, 2, true,
            priorityReduce, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 3 * GB, 2, true,
            priorityMap, recordFactory)));

    // Start testing...
    // Only AM
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(22 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getUsedResource().getMemory());
    assertEquals(0 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());

    // Only 1 map - simulating reduce
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(5 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(19 * GB, a.getMetrics().getAvailableMB());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(0 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());

    // Only 1 map to other node - simulating reduce
    a.assignContainers(clusterResource, node_1, false);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(16 * GB, a.getMetrics().getAvailableMB());
    assertEquals(16 * GB, app_0.getHeadroom().getMemory());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());
    assertEquals(2, app_0.getTotalRequiredResources(priorityReduce));

    // try to assign reducer (5G on node 0 and should reserve)
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(11 * GB, a.getMetrics().getAvailableMB());
    assertEquals(11 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getReservedContainer().getReservedResource()
        .getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());
    assertEquals(2, app_0.getTotalRequiredResources(priorityReduce));

    // assign reducer to node 2
    a.assignContainers(clusterResource, node_2, false);
    assertEquals(18 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(6 * GB, a.getMetrics().getAvailableMB());
    assertEquals(6 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getReservedContainer().getReservedResource()
        .getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(5 * GB, node_2.getUsedResource().getMemory());
    assertEquals(1, app_0.getTotalRequiredResources(priorityReduce));

    // node_1 heartbeat and won't unreserve from node_0, potentially stuck
    // if AM doesn't handle
    a.assignContainers(clusterResource, node_1, false);
    assertEquals(18 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(6 * GB, a.getMetrics().getAvailableMB());
    assertEquals(6 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getReservedContainer().getReservedResource()
        .getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(5 * GB, node_2.getUsedResource().getMemory());
    assertEquals(1, app_0.getTotalRequiredResources(priorityReduce));
  }

  @Test
  public void testAssignContainersNeedToUnreserve() throws Exception {
    // Test that we now unreserve and use a node that has space

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), rmContext);

    a.submitApplicationAttempt(app_0, user_0); 

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), rmContext);
    a.submitApplicationAttempt(app_1, user_0); 

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);

    final int numNodes = 2;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 5 * GB, 2, true,
            priorityReduce, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 3 * GB, 2, true,
            priorityMap, recordFactory)));

    // Start testing...
    // Only AM
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(14 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getUsedResource().getMemory());
    assertEquals(0 * GB, node_1.getUsedResource().getMemory());

    // Only 1 map - simulating reduce
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(5 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(11 * GB, a.getMetrics().getAvailableMB());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(0 * GB, node_1.getUsedResource().getMemory());

    // Only 1 map to other node - simulating reduce
    a.assignContainers(clusterResource, node_1, false);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(8 * GB, a.getMetrics().getAvailableMB());
    assertEquals(8 * GB, app_0.getHeadroom().getMemory());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(2, app_0.getTotalRequiredResources(priorityReduce));

    // try to assign reducer (5G on node 0 and should reserve)
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(3 * GB, a.getMetrics().getAvailableMB());
    assertEquals(3 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getReservedContainer().getReservedResource()
        .getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(2, app_0.getTotalRequiredResources(priorityReduce));

    // could allocate but told need to unreserve first
    a.assignContainers(clusterResource, node_1, true);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(3 * GB, a.getMetrics().getAvailableMB());
    assertEquals(3 * GB, app_0.getHeadroom().getMemory());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(8 * GB, node_1.getUsedResource().getMemory());
    assertEquals(1, app_0.getTotalRequiredResources(priorityReduce));
  }

  @Test
  public void testGetAppToUnreserve() throws Exception {

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);
    final String user_0 = "user_0";
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), rmContext);

    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);

    // Setup resource-requests
    Priority priorityMap = TestUtils.createMockPriority(5);
    Resource capability = Resources.createResource(2*GB, 0);

    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    RMContext rmContext = mock(RMContext.class);
    ContainerAllocationExpirer expirer =
      mock(ContainerAllocationExpirer.class);
    DrainDispatcher drainDispatcher = new DrainDispatcher();
    when(rmContext.getContainerAllocationExpirer()).thenReturn(expirer);
    when(rmContext.getDispatcher()).thenReturn(drainDispatcher);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        app_0.getApplicationId(), 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);
    Container container = TestUtils.getMockContainer(containerId,
        node_1.getNodeID(), Resources.createResource(2*GB), priorityMap);
    RMContainer rmContainer = new RMContainerImpl(container, appAttemptId,
        node_1.getNodeID(), "user", rmContext);

    Container container_1 = TestUtils.getMockContainer(containerId,
        node_0.getNodeID(), Resources.createResource(1*GB), priorityMap);
    RMContainer rmContainer_1 = new RMContainerImpl(container_1, appAttemptId,
        node_0.getNodeID(), "user", rmContext);

    // no reserved containers
    NodeId unreserveId = app_0.getNodeIdToUnreserve(priorityMap, capability);
    assertEquals(null, unreserveId);

    // no reserved containers - reserve then unreserve
    app_0.reserve(node_0, priorityMap, rmContainer_1, container_1);
    app_0.unreserve(node_0, priorityMap);
    unreserveId = app_0.getNodeIdToUnreserve(priorityMap, capability);
    assertEquals(null, unreserveId);

    // no container large enough is reserved
    app_0.reserve(node_0, priorityMap, rmContainer_1, container_1);
    unreserveId = app_0.getNodeIdToUnreserve(priorityMap, capability);
    assertEquals(null, unreserveId);

    // reserve one that is now large enough
    app_0.reserve(node_1, priorityMap, rmContainer, container);
    unreserveId = app_0.getNodeIdToUnreserve(priorityMap, capability);
    assertEquals(node_1.getNodeID(), unreserveId);
  }

  @Test
  public void testFindNodeToUnreserve() throws Exception {

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);
    final String user_0 = "user_0";
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), rmContext);

    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);

    // Setup resource-requests
    Priority priorityMap = TestUtils.createMockPriority(5);
    Resource capability = Resources.createResource(2 * GB, 0);

    RMApplicationHistoryWriter writer = mock(RMApplicationHistoryWriter.class);
    SystemMetricsPublisher publisher = mock(SystemMetricsPublisher.class);
    RMContext rmContext = mock(RMContext.class);
    ContainerAllocationExpirer expirer =
      mock(ContainerAllocationExpirer.class);
    DrainDispatcher drainDispatcher = new DrainDispatcher();
    when(rmContext.getContainerAllocationExpirer()).thenReturn(expirer);
    when(rmContext.getDispatcher()).thenReturn(drainDispatcher);
    when(rmContext.getRMApplicationHistoryWriter()).thenReturn(writer);
    when(rmContext.getSystemMetricsPublisher()).thenReturn(publisher);
    ApplicationAttemptId appAttemptId = BuilderUtils.newApplicationAttemptId(
        app_0.getApplicationId(), 1);
    ContainerId containerId = BuilderUtils.newContainerId(appAttemptId, 1);
    Container container = TestUtils.getMockContainer(containerId,
        node_1.getNodeID(), Resources.createResource(2*GB), priorityMap);
    RMContainer rmContainer = new RMContainerImpl(container, appAttemptId,
        node_1.getNodeID(), "user", rmContext);

    // nothing reserved
    boolean res = a.findNodeToUnreserve(csContext.getClusterResource(),
        node_1, app_0, priorityMap, capability);
    assertFalse(res);

    // reserved but scheduler doesn't know about that node.
    app_0.reserve(node_1, priorityMap, rmContainer, container);
    node_1.reserveResource(app_0, priorityMap, rmContainer);
    res = a.findNodeToUnreserve(csContext.getClusterResource(), node_1, app_0,
        priorityMap, capability);
    assertFalse(res);
  }

  @Test
  public void testAssignToQueue() throws Exception {

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), rmContext);

    a.submitApplicationAttempt(app_0, user_0); 

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), rmContext);
    a.submitApplicationAttempt(app_1, user_0); 

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);
    String host_2 = "host_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, DEFAULT_RACK, 0,
        8 * GB);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getNode(node_2.getNodeID())).thenReturn(node_2);

    final int numNodes = 2;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 5 * GB, 2, true,
            priorityReduce, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 3 * GB, 2, true,
            priorityMap, recordFactory)));

    // Start testing...
    // Only AM
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(14 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getUsedResource().getMemory());
    assertEquals(0 * GB, node_1.getUsedResource().getMemory());

    // Only 1 map - simulating reduce
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(5 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(11 * GB, a.getMetrics().getAvailableMB());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(0 * GB, node_1.getUsedResource().getMemory());

    // Only 1 map to other node - simulating reduce
    a.assignContainers(clusterResource, node_1, false);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(8 * GB, a.getMetrics().getAvailableMB());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());

    // allocate to queue so that the potential new capacity is greater then
    // absoluteMaxCapacity
    Resource capability = Resources.createResource(32 * GB, 0);
    boolean res =
        a.canAssignToThisQueue(clusterResource, capability,
            CommonNodeLabelsManager.EMPTY_STRING_SET, app_0, true);
    assertFalse(res);

    // now add in reservations and make sure it continues if config set
    // allocate to queue so that the potential new capacity is greater then
    // absoluteMaxCapacity
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(3 * GB, a.getMetrics().getAvailableMB());
    assertEquals(3 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());

    capability = Resources.createResource(5 * GB, 0);
    res =
        a.canAssignToThisQueue(clusterResource, capability,
            CommonNodeLabelsManager.EMPTY_STRING_SET, app_0, true);
    assertTrue(res);

    // tell to not check reservations
    res =
        a.canAssignToThisQueue(clusterResource, capability,
            CommonNodeLabelsManager.EMPTY_STRING_SET, app_0, false);
    assertFalse(res);

    refreshQueuesTurnOffReservationsContLook(a, csConf);

    // should return false no matter what checkReservations is passed
    // in since feature is off
    res =
        a.canAssignToThisQueue(clusterResource, capability,
            CommonNodeLabelsManager.EMPTY_STRING_SET, app_0, false);
    assertFalse(res);

    res =
        a.canAssignToThisQueue(clusterResource, capability,
            CommonNodeLabelsManager.EMPTY_STRING_SET, app_0, true);
    assertFalse(res);
  }

  public void refreshQueuesTurnOffReservationsContLook(LeafQueue a,
      CapacitySchedulerConfiguration csConf) throws Exception {
    // before reinitialization
    assertEquals(true, a.getReservationContinueLooking());
    assertEquals(true,
        ((ParentQueue) a.getParent()).getReservationContinueLooking());

    csConf.setBoolean(
        CapacitySchedulerConfiguration.RESERVE_CONT_LOOK_ALL_NODES, false);
    Map<String, CSQueue> newQueues = new HashMap<String, CSQueue>();
    CSQueue newRoot = CapacityScheduler.parseQueue(csContext, csConf, null,
        CapacitySchedulerConfiguration.ROOT, newQueues, queues,
        TestUtils.spyHook);
    queues = newQueues;
    root.reinitialize(newRoot, cs.getClusterResource());

    // after reinitialization
    assertEquals(false, a.getReservationContinueLooking());
    assertEquals(false,
        ((ParentQueue) a.getParent()).getReservationContinueLooking());
  }

  @Test
  public void testContinueLookingReservationsAfterQueueRefresh()
      throws Exception {
    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);

    // Manipulate queue 'e'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    refreshQueuesTurnOffReservationsContLook(a, csConf);
  }

  @Test
  public void testAssignToUser() throws Exception {

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), rmContext);

    a.submitApplicationAttempt(app_0, user_0); 

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), rmContext);
    a.submitApplicationAttempt(app_1, user_0); 

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);
    String host_2 = "host_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, DEFAULT_RACK, 0,
        8 * GB);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getNode(node_2.getNodeID())).thenReturn(node_2);

    final int numNodes = 2;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 5 * GB, 2, true,
            priorityReduce, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 3 * GB, 2, true,
            priorityMap, recordFactory)));

    // Start testing...
    // Only AM
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(14 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getUsedResource().getMemory());
    assertEquals(0 * GB, node_1.getUsedResource().getMemory());

    // Only 1 map - simulating reduce
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(5 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(11 * GB, a.getMetrics().getAvailableMB());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(0 * GB, node_1.getUsedResource().getMemory());

    // Only 1 map to other node - simulating reduce
    a.assignContainers(clusterResource, node_1, false);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(8 * GB, a.getMetrics().getAvailableMB());
    assertEquals(null, node_0.getReservedContainer());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());

    // now add in reservations and make sure it continues if config set
    // allocate to queue so that the potential new capacity is greater then
    // absoluteMaxCapacity
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(5 * GB, app_0.getCurrentReservation().getMemory());

    assertEquals(5 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(3 * GB, a.getMetrics().getAvailableMB());
    assertEquals(3 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());

    // set limit so subtrace reservations it can continue
    Resource limit = Resources.createResource(12 * GB, 0);
    boolean res = a.assignToUser(clusterResource, user_0, limit, app_0,
        true, null);
    assertTrue(res);

    // tell it not to check for reservations and should fail as already over
    // limit
    res = a.assignToUser(clusterResource, user_0, limit, app_0, false, null);
    assertFalse(res);

    refreshQueuesTurnOffReservationsContLook(a, csConf);

    // should now return false since feature off
    res = a.assignToUser(clusterResource, user_0, limit, app_0, true, null);
    assertFalse(res);
  }

  @Test
  public void testReservationsNoneAvailable() throws Exception {
    // Test that we now unreserve and use a node that has space

    CapacitySchedulerConfiguration csConf = new CapacitySchedulerConfiguration();
    setup(csConf);

    // Manipulate queue 'a'
    LeafQueue a = stubLeafQueue((LeafQueue) queues.get(A));

    // Users
    final String user_0 = "user_0";

    // Submit applications
    final ApplicationAttemptId appAttemptId_0 = TestUtils
        .getMockApplicationAttemptId(0, 0);
    FiCaSchedulerApp app_0 = new FiCaSchedulerApp(appAttemptId_0, user_0, a,
        mock(ActiveUsersManager.class), rmContext);

    a.submitApplicationAttempt(app_0, user_0); 

    final ApplicationAttemptId appAttemptId_1 = TestUtils
        .getMockApplicationAttemptId(1, 0);
    FiCaSchedulerApp app_1 = new FiCaSchedulerApp(appAttemptId_1, user_0, a,
        mock(ActiveUsersManager.class), rmContext);
    a.submitApplicationAttempt(app_1, user_0); 

    // Setup some nodes
    String host_0 = "host_0";
    FiCaSchedulerNode node_0 = TestUtils.getMockNode(host_0, DEFAULT_RACK, 0,
        8 * GB);
    String host_1 = "host_1";
    FiCaSchedulerNode node_1 = TestUtils.getMockNode(host_1, DEFAULT_RACK, 0,
        8 * GB);
    String host_2 = "host_2";
    FiCaSchedulerNode node_2 = TestUtils.getMockNode(host_2, DEFAULT_RACK, 0,
        8 * GB);

    when(csContext.getNode(node_0.getNodeID())).thenReturn(node_0);
    when(csContext.getNode(node_1.getNodeID())).thenReturn(node_1);
    when(csContext.getNode(node_2.getNodeID())).thenReturn(node_2);

    final int numNodes = 3;
    Resource clusterResource = Resources.createResource(numNodes * (8 * GB));
    when(csContext.getNumClusterNodes()).thenReturn(numNodes);

    // Setup resource-requests
    Priority priorityAM = TestUtils.createMockPriority(1);
    Priority priorityMap = TestUtils.createMockPriority(5);
    Priority priorityReduce = TestUtils.createMockPriority(10);
    Priority priorityLast = TestUtils.createMockPriority(12);

    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 2 * GB, 1, true,
            priorityAM, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 5 * GB, 1, true,
            priorityReduce, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 3 * GB, 2, true,
            priorityMap, recordFactory)));
    app_0.updateResourceRequests(Collections.singletonList(TestUtils
        .createResourceRequest(ResourceRequest.ANY, 8 * GB, 2, true,
            priorityLast, recordFactory)));

    // Start testing...
    // Only AM
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(2 * GB, a.getUsedResources().getMemory());
    assertEquals(2 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(2 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(22 * GB, a.getMetrics().getAvailableMB());
    assertEquals(2 * GB, node_0.getUsedResource().getMemory());
    assertEquals(0 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());

    // Only 1 map - simulating reduce
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(5 * GB, a.getUsedResources().getMemory());
    assertEquals(5 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(5 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(19 * GB, a.getMetrics().getAvailableMB());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(0 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());

    // Only 1 map to other node - simulating reduce
    a.assignContainers(clusterResource, node_1, false);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(16 * GB, a.getMetrics().getAvailableMB());
    assertEquals(16 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());

    // try to assign reducer (5G on node 0), but tell it
    // it has to unreserve. No room to allocate and shouldn't reserve
    // since nothing currently reserved.
    a.assignContainers(clusterResource, node_0, true);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(16 * GB, a.getMetrics().getAvailableMB());
    assertEquals(16 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());

    // try to assign reducer (5G on node 2), but tell it
    // it has to unreserve. Has room but shouldn't reserve
    // since nothing currently reserved.
    a.assignContainers(clusterResource, node_2, true);
    assertEquals(8 * GB, a.getUsedResources().getMemory());
    assertEquals(8 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(8 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(16 * GB, a.getMetrics().getAvailableMB());
    assertEquals(16 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(0 * GB, node_2.getUsedResource().getMemory());

    // let it assign 5G to node_2
    a.assignContainers(clusterResource, node_2, false);
    assertEquals(13 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(0 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(11 * GB, a.getMetrics().getAvailableMB());
    assertEquals(11 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(5 * GB, node_2.getUsedResource().getMemory());

    // reserve 8G node_0
    a.assignContainers(clusterResource, node_0, false);
    assertEquals(21 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(8 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(3 * GB, a.getMetrics().getAvailableMB());
    assertEquals(3 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(5 * GB, node_2.getUsedResource().getMemory());

    // try to assign (8G on node 2). No room to allocate,
    // continued to try due to having reservation above,
    // but hits queue limits so can't reserve anymore.
    a.assignContainers(clusterResource, node_2, false);
    assertEquals(21 * GB, a.getUsedResources().getMemory());
    assertEquals(13 * GB, app_0.getCurrentConsumption().getMemory());
    assertEquals(8 * GB, a.getMetrics().getReservedMB());
    assertEquals(13 * GB, a.getMetrics().getAllocatedMB());
    assertEquals(3 * GB, a.getMetrics().getAvailableMB());
    assertEquals(3 * GB, app_0.getHeadroom().getMemory());
    assertEquals(5 * GB, node_0.getUsedResource().getMemory());
    assertEquals(3 * GB, node_1.getUsedResource().getMemory());
    assertEquals(5 * GB, node_2.getUsedResource().getMemory());
  }
}
