/*******************************************************************************
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *  
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *******************************************************************************/
package org.apache.hadoop.yarn.server.resourcemanager.reservation;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.yarn.api.records.ReservationDefinition;
import org.apache.hadoop.yarn.api.records.ReservationId;
import org.apache.hadoop.yarn.api.records.ReservationRequest;
import org.apache.hadoop.yarn.api.records.ReservationRequestInterpreter;
import org.apache.hadoop.yarn.api.records.ReservationRequests;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationDefinitionPBImpl;
import org.apache.hadoop.yarn.api.records.impl.pb.ReservationRequestsPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.reservation.exceptions.PlanningException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.log.Log;

public class TestGreedyReservationAgent {

  ReservationAgent agent;
  InMemoryPlan plan;
  Resource minAlloc = Resource.newInstance(1024, 1);
  ResourceCalculator res = new DefaultResourceCalculator();
  Resource maxAlloc = Resource.newInstance(1024 * 8, 8);
  Random rand = new Random();
  long step;

  @Before
  public void setup() throws Exception {

    long seed = rand.nextLong();
    rand.setSeed(seed);
    Log.info("Running with seed: " + seed);

    // setting completely loose quotas
    long timeWindow = 1000000L;
    Resource clusterCapacity = Resource.newInstance(100 * 1024, 100);
    step = 1000L;
    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
    CapacityScheduler scheduler = testUtil.mockCapacityScheduler(125);
    String reservationQ = testUtil.getFullReservationQueueName();
    CapacitySchedulerConfiguration capConf = scheduler.getConfiguration();
    capConf.setReservationWindow(reservationQ, timeWindow);
    capConf.setMaximumCapacity(reservationQ, 100);
    capConf.setAverageCapacity(reservationQ, 100);
    CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
    policy.init(reservationQ, capConf);
    agent = new GreedyReservationAgent();

    QueueMetrics queueMetrics = QueueMetrics.forQueue("dedicated",
        mock(ParentQueue.class), false, capConf);

    plan = new InMemoryPlan(queueMetrics, policy, agent, clusterCapacity, step,
        res, minAlloc, maxAlloc, "dedicated", null, true);
  }

  @SuppressWarnings("javadoc")
  @Test
  public void testSimple() throws PlanningException {

    prepareBasicPlan();

    // create a request with a single atomic ask
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(5 * step);
    rr.setDeadline(20 * step);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 10, 5, 10 * step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setReservationResources(Collections.singletonList(r));
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr);

    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 3);

    ReservationAllocation cs = plan.getReservationById(reservationID);

    System.out.println("--------AFTER SIMPLE ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

    for (long i = 10 * step; i < 20 * step; i++) {
      assertTrue(
          "Agent-based allocation unexpected",
          Resources.equals(cs.getResourcesAtTime(i),
              Resource.newInstance(2048 * 10, 2 * 10)));
    }

  }

  @Test
  public void testOrder() throws PlanningException {
    prepareBasicPlan();

    // create a completely utilized segment around time 30
    int[] f = { 100, 100 };

    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", 30 * step, 30 * step + f.length * step,
            ReservationSystemTestUtil.generateAllocation(30 * step, step, f),
            res, minAlloc)));

    // create a chain of 4 RR, mixing gang and non-gang
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(0 * step);
    rr.setDeadline(70 * step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ORDER);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 10, 1, 10 * step);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 10, 10, 20 * step);
    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    list.add(r2);
    list.add(r);
    list.add(r2);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    // submit to agent
    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr);

    // validate
    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 4);

    ReservationAllocation cs = plan.getReservationById(reservationID);

    assertTrue(cs.toString(), check(cs, 0 * step, 10 * step, 20, 1024, 1));
    assertTrue(cs.toString(), check(cs, 10 * step, 30 * step, 10, 1024, 1));
    assertTrue(cs.toString(), check(cs, 40 * step, 50 * step, 20, 1024, 1));
    assertTrue(cs.toString(), check(cs, 50 * step, 70 * step, 10, 1024, 1));

    System.out.println("--------AFTER ORDER ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  @Test
  public void testOrderNoGapImpossible() throws PlanningException {
    prepareBasicPlan();
    // create a completely utilized segment at time 30
    int[] f = { 100, 100 };

    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", 30 * step, 30 * step + f.length * step,
            ReservationSystemTestUtil.generateAllocation(30 * step, step, f),
            res, minAlloc)));

    // create a chain of 4 RR, mixing gang and non-gang
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(0L);

    rr.setDeadline(70L);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ORDER_NO_GAP);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 10, 1, 10);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 10, 10, 20);
    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    list.add(r2);
    list.add(r);
    list.add(r2);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    boolean result = false;
    try {
      // submit to agent
      result = agent.createReservation(reservationID, "u1", plan, rr);
      fail();
    } catch (PlanningException p) {
      // expected
    }

    // validate
    assertFalse("Agent-based allocation should have failed", result);
    assertTrue("Agent-based allocation should have failed", plan
        .getAllReservations().size() == 3);

    System.out
        .println("--------AFTER ORDER_NO_GAP IMPOSSIBLE ALLOCATION (queue: "
            + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  @Test
  public void testOrderNoGap() throws PlanningException {
    prepareBasicPlan();
    // create a chain of 4 RR, mixing gang and non-gang
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(0 * step);
    rr.setDeadline(60 * step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ORDER_NO_GAP);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 10, 1, 10 * step);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 10, 10, 20 * step);
    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    list.add(r2);
    list.add(r);
    list.add(r2);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);
    rr.setReservationRequests(reqs);

    // submit to agent
    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr);

    System.out.println("--------AFTER ORDER ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

    // validate
    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 3);

    ReservationAllocation cs = plan.getReservationById(reservationID);

    assertTrue(cs.toString(), check(cs, 0 * step, 10 * step, 20, 1024, 1));
    assertTrue(cs.toString(), check(cs, 10 * step, 30 * step, 10, 1024, 1));
    assertTrue(cs.toString(), check(cs, 30 * step, 40 * step, 20, 1024, 1));
    assertTrue(cs.toString(), check(cs, 40 * step, 60 * step, 10, 1024, 1));

  }

  @Test
  public void testSingleSliding() throws PlanningException {
    prepareBasicPlan();

    // create a single request for which we need subsequent (tight) packing.
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(100 * step);
    rr.setDeadline(120 * step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 200, 10, 10 * step);

    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    // submit to agent
    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr);

    // validate results, we expect the second one to be accepted
    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 3);

    ReservationAllocation cs = plan.getReservationById(reservationID);

    assertTrue(cs.toString(), check(cs, 100 * step, 120 * step, 100, 1024, 1));

    System.out.println("--------AFTER packed ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  @Test
  public void testAny() throws PlanningException {
    prepareBasicPlan();
    // create an ANY request, with an impossible step (last in list, first
    // considered),
    // and two satisfiable ones. We expect the second one to be returned.

    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(100 * step);
    rr.setDeadline(120 * step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ANY);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 5, 5, 10 * step);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 10, 5, 10 * step);
    ReservationRequest r3 = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 110, 110, 10 * step);

    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    list.add(r2);
    list.add(r3);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    // submit to agent
    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    boolean res = agent.createReservation(reservationID, "u1", plan, rr);

    // validate results, we expect the second one to be accepted
    assertTrue("Agent-based allocation failed", res);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 3);

    ReservationAllocation cs = plan.getReservationById(reservationID);

    assertTrue(cs.toString(), check(cs, 110 * step, 120 * step, 20, 1024, 1));

    System.out.println("--------AFTER ANY ALLOCATION (queue: " + reservationID
        + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  @Test
  public void testAnyImpossible() throws PlanningException {
    prepareBasicPlan();
    // create an ANY request, with all impossible alternatives
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(100L);
    rr.setDeadline(120L);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ANY);

    // longer than arrival-deadline
    ReservationRequest r1 = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 35, 5, 30);
    // above max cluster size
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 110, 110, 10);

    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r1);
    list.add(r2);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    boolean result = false;
    try {
      // submit to agent
      result = agent.createReservation(reservationID, "u1", plan, rr);
      fail();
    } catch (PlanningException p) {
      // expected
    }
    // validate results, we expect the second one to be accepted
    assertFalse("Agent-based allocation should have failed", result);
    assertTrue("Agent-based allocation should have failed", plan
        .getAllReservations().size() == 2);

    System.out.println("--------AFTER ANY IMPOSSIBLE ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  @Test
  public void testAll() throws PlanningException {
    prepareBasicPlan();
    // create an ALL request
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(100 * step);
    rr.setDeadline(120 * step);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 5, 5, 10 * step);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 10, 10, 20 * step);

    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    list.add(r2);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    // submit to agent
    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    agent.createReservation(reservationID, "u1", plan, rr);

    // validate results, we expect the second one to be accepted
    assertTrue("Agent-based allocation failed", reservationID != null);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 3);

    ReservationAllocation cs = plan.getReservationById(reservationID);

    assertTrue(cs.toString(), check(cs, 100 * step, 110 * step, 20, 1024, 1));
    assertTrue(cs.toString(), check(cs, 110 * step, 120 * step, 25, 1024, 1));

    System.out.println("--------AFTER ALL ALLOCATION (queue: " + reservationID
        + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  @Test
  public void testAllImpossible() throws PlanningException {
    prepareBasicPlan();
    // create an ALL request, with an impossible combination, it should be
    // rejected, and allocation remain unchanged
    ReservationDefinition rr = new ReservationDefinitionPBImpl();
    rr.setArrival(100L);
    rr.setDeadline(120L);
    ReservationRequests reqs = new ReservationRequestsPBImpl();
    reqs.setInterpreter(ReservationRequestInterpreter.R_ALL);
    ReservationRequest r = ReservationRequest.newInstance(
        Resource.newInstance(1024, 1), 55, 5, 10);
    ReservationRequest r2 = ReservationRequest.newInstance(
        Resource.newInstance(2048, 2), 55, 5, 20);

    List<ReservationRequest> list = new ArrayList<ReservationRequest>();
    list.add(r);
    list.add(r2);
    reqs.setReservationResources(list);
    rr.setReservationRequests(reqs);

    ReservationId reservationID = ReservationSystemTestUtil
        .getNewReservationId();
    boolean result = false;
    try {
      // submit to agent
      result = agent.createReservation(reservationID, "u1", plan, rr);
      fail();
    } catch (PlanningException p) {
      // expected
    }

    // validate results, we expect the second one to be accepted
    assertFalse("Agent-based allocation failed", result);
    assertTrue("Agent-based allocation failed", plan.getAllReservations()
        .size() == 2);

    System.out.println("--------AFTER ALL IMPOSSIBLE ALLOCATION (queue: "
        + reservationID + ")----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());

  }

  private void prepareBasicPlan() throws PlanningException {

    // insert in the reservation a couple of controlled reservations, to create
    // conditions for assignment that are non-empty

    int[] f = { 10, 10, 20, 20, 20, 10, 10 };

    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", 0L, 0L + f.length * step, ReservationSystemTestUtil
                .generateAllocation(0, step, f), res, minAlloc)));

    int[] f2 = { 5, 5, 5, 5, 5, 5, 5 };
    Map<ReservationInterval, ReservationRequest> alloc = 
        ReservationSystemTestUtil.generateAllocation(5000, step, f2);
    assertTrue(plan.toString(),
        plan.addReservation(new InMemoryReservationAllocation(
            ReservationSystemTestUtil.getNewReservationId(), null, "u1",
            "dedicated", 5000, 5000 + f2.length * step, alloc, res, minAlloc)));

    System.out.println("--------BEFORE AGENT----------");
    System.out.println(plan.toString());
    System.out.println(plan.toCumulativeString());
  }

  private boolean check(ReservationAllocation cs, long start, long end,
      int containers, int mem, int cores) {

    boolean res = true;
    for (long i = start; i < end; i++) {
      res = res
          && Resources.equals(cs.getResourcesAtTime(i),
              Resource.newInstance(mem * containers, cores * containers));
    }
    return res;
  }

  public void testStress(int numJobs) throws PlanningException, IOException {

    long timeWindow = 1000000L;
    Resource clusterCapacity = Resource.newInstance(500 * 100 * 1024, 500 * 32);
    step = 1000L;
    ReservationSystemTestUtil testUtil = new ReservationSystemTestUtil();
    CapacityScheduler scheduler = testUtil.mockCapacityScheduler(500 * 100);
    String reservationQ = testUtil.getFullReservationQueueName();
    CapacitySchedulerConfiguration capConf = scheduler.getConfiguration();
    capConf.setReservationWindow(reservationQ, timeWindow);
    capConf.setMaximumCapacity(reservationQ, 100);
    capConf.setAverageCapacity(reservationQ, 100);
    CapacityOverTimePolicy policy = new CapacityOverTimePolicy();
    policy.init(reservationQ, capConf);

    plan = new InMemoryPlan(scheduler.getRootQueueMetrics(), policy, agent,
      clusterCapacity, step, res, minAlloc, maxAlloc, "dedicated", null, true);

    int acc = 0;
    List<ReservationDefinition> list = new ArrayList<ReservationDefinition>();
    for (long i = 0; i < numJobs; i++) {
      list.add(ReservationSystemTestUtil.generateRandomRR(rand, i));
    }

    long start = System.currentTimeMillis();
    for (int i = 0; i < numJobs; i++) {

      try {
        if (agent.createReservation(
            ReservationSystemTestUtil.getNewReservationId(), "u" + i % 100,
            plan, list.get(i))) {
          acc++;
        }
      } catch (PlanningException p) {
        // ignore exceptions
      }
    }

    long end = System.currentTimeMillis();
    System.out.println("Submitted " + numJobs + " jobs " + " accepted " + acc
        + " in " + (end - start) + "ms");
  }

  public static void main(String[] arg) {

    // run a stress test with by default 1000 random jobs
    int numJobs = 1000;
    if (arg.length > 0) {
      numJobs = Integer.parseInt(arg[0]);
    }

    try {
      TestGreedyReservationAgent test = new TestGreedyReservationAgent();
      test.setup();
      test.testStress(numJobs);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
