/*
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

package org.apache.slider.server.appmaster.model.appstate

import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.model.mock.MockRMOperationHandler
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.operations.CancelSingleRequest
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation
import org.apache.slider.server.appmaster.operations.RMOperationHandler
import org.apache.slider.server.appmaster.state.AppState
import org.apache.slider.server.appmaster.state.ContainerAssignment
import org.apache.slider.server.appmaster.state.RoleInstance
import org.junit.Test

import static org.apache.slider.server.appmaster.state.ContainerPriority.buildPriority
import static org.apache.slider.server.appmaster.state.ContainerPriority.extractRole

@Slf4j
class TestMockAppStateRMOperations extends BaseMockAppStateTest implements MockRoles {

  @Override
  String getTestName() {
    return "TestMockAppStateRMOperations"
  }

  @Test
  public void testPriorityOnly() throws Throwable {
    assert 5 == extractRole(buildPriority(5, false))
  }

  @Test
  public void testPriorityRoundTrip() throws Throwable {
    assert 5 == extractRole(buildPriority(5, false))
  }

  @Test
  public void testPriorityRoundTripWithRequest() throws Throwable {
    int priority = buildPriority(5, false)
    assert 5 == extractRole(priority)
  }

  @Test
  public void testMockAddOp() throws Throwable {
    role0Status.desired = 1
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assertListLength(ops, 1)
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    int priority = operation.request.priority.priority
    assert extractRole(priority) == MockFactory.PROVIDER_ROLE0.id
    RMOperationHandler handler = new MockRMOperationHandler()
    handler.execute(ops)

    AbstractRMOperation op = handler.operations[0]
    assert op instanceof ContainerRequestOperation
  }

  /**
   * Test of a flex up and down op which verifies that outstanding
   * requests are cancelled first.
   * <ol>
   *   <li>request 5 nodes, assert 5 request made</li>
   *   <li>allocate 1 of them</li>
   *   <li>flex cluster size to 3</li>
   *   <li>assert this generates 2 cancel requests</li>
   * </ol>
   */
  @Test
  public void testRequestThenCancelOps() throws Throwable {
    def role0 = role0Status
    role0.desired = 5
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assertListLength(ops, 5)
    // now 5 outstanding requests.
    assert role0.requested == 5

    // allocate one
    List<AbstractRMOperation> processed = [ops[0]]
    List<ContainerId> released = []
    List<AppState.NodeCompletionResult> completionResults = []
    submitOperations(processed, released)
    List<RoleInstance> instances = createAndSubmitNodes(released)
    processSubmissionOperations(instances, completionResults, released)


    // four outstanding
    assert role0.requested == 4


    // flex cluster to 3
    role0.desired = 3
    ops = appState.reviewRequestAndReleaseNodes()

    // expect two cancel operation from review
    assertListLength(ops, 2)
    ops.each { assert it instanceof CancelSingleRequest }

    RMOperationHandler handler = new MockRMOperationHandler()
    handler.availableToCancel = 4;
    handler.execute(ops)
    assert handler.availableToCancel == 2
    assert role0.requested == 2
    
    // flex down one more
    role0.desired = 2
    ops = appState.reviewRequestAndReleaseNodes()
    assertListLength(ops, 1)
    ops.each { assert it instanceof CancelSingleRequest }
    handler.execute(ops)
    assert handler.availableToCancel == 1
    assert role0.requested == 1
  }

  @Test
  public void testCancelNoActualContainers() throws Throwable {
    def role0 = role0Status
    role0.desired = 5
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assertListLength(ops, 5)
    // now 5 outstanding requests.
    assert role0.requested == 5
    role0.desired = 0
    ops = appState.reviewRequestAndReleaseNodes()
    assertListLength(ops, 5)

  }


  @Test
  public void testFlexDownOutstandingRequests() throws Throwable {
    // engine only has two nodes, so > 2 will be outstanding
    engine = new MockYarnEngine(1, 2)
    List<AbstractRMOperation> ops
    // role: desired = 2, requested = 1, actual=1 
    def role0 = role0Status
    role0.desired = 4
    createAndSubmitNodes()
    
    assert role0.requested == 2
    assert role0.actual == 2
    // there are now two outstanding, two actual 
    // Release 3 and verify that the two
    // cancellations were combined with a release
    role0.desired = 1;
    assert role0.delta == -3
    ops = appState.reviewRequestAndReleaseNodes()
    assertListLength(ops, 3)
    assert 2 == (ops.findAll {it instanceof CancelSingleRequest}).size()
    assert 1 == (ops.findAll {it instanceof ContainerReleaseOperation}).size()
    assert role0.requested == 0
    assert role0.releasing == 1
  }

  @Test
  public void testCancelAllOutstandingRequests() throws Throwable {

    // role: desired = 2, requested = 1, actual=1 
    def role0 = role0Status
    role0.desired = 2
    List<AbstractRMOperation> ops
    ops = appState.reviewRequestAndReleaseNodes()
    assert 2 == (ops.findAll { it instanceof ContainerRequestOperation }).size()

    // there are now two outstanding, two actual
    // Release 3 and verify that the two
    // cancellations were combined with a release
    role0.desired = 0;
    ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 2
    assert 2 == (ops.findAll { it instanceof CancelSingleRequest }).size()
  }

  
  @Test
  public void testFlexUpOutstandingRequests() throws Throwable {

    List<AbstractRMOperation> ops
    // role: desired = 2, requested = 1, actual=1
    def role0 = role0Status
    role0.desired = 2
    role0.incActual();
    role0.incRequested()



    // flex up 2 nodes, yet expect only one node to be requested,
    // as the  outstanding request is taken into account
    role0.desired = 4;
    role0.incRequested()

    assert role0.actual == 1;
    assert role0.requested == 2;
    assert role0.actualAndRequested == 3;
    assert role0.delta == 1
    ops = appState.reviewRequestAndReleaseNodes()
    assertListLength(ops, 1)
    assert ops[0] instanceof ContainerRequestOperation
    assert role0.requested == 3
  }

  @Test
  public void testFlexUpNoSpace() throws Throwable {
    // engine only has two nodes, so > 2 will be outstanding
    engine = new MockYarnEngine(1, 2)
    List<AbstractRMOperation> ops
    // role: desired = 2, requested = 1, actual=1 
    def role0 = role0Status
    role0.desired = 4
    createAndSubmitNodes()

    assert role0.requested == 2
    assert role0.actual == 2
    role0.desired = 8;
    assert role0.delta == 4
    createAndSubmitNodes()
    assert role0.requested == 6
  }


  @Test
  public void testAllocateReleaseOp() throws Throwable {
    role0Status.desired = 1

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    def (ArrayList<ContainerAssignment> assignments, Container cont, AMRMClient.ContainerRequest request) = satisfyContainerRequest(
        operation)
    assertListLength(ops, 1)
    assertListLength(assignments, 1)
    ContainerAssignment assigned = assignments[0]
    Container target = assigned.container
    assert target.id == cont.id
    int roleId = assigned.role.priority
    assert roleId == extractRole(request.priority)
    assert assigned.role.name == ROLE0
    RoleInstance ri = roleInstance(assigned)
    //tell the app it arrived
    appState.containerStartSubmitted(target, ri);
    appState.innerOnNodeManagerContainerStarted(target.id)
    assert role0Status.started == 1

    //now release it by changing the role status
    role0Status.desired = 0
    ops = appState.reviewRequestAndReleaseNodes()
    assertListLength(ops, 1)

    assert ops[0] instanceof ContainerReleaseOperation
    ContainerReleaseOperation release = (ContainerReleaseOperation) ops[0]
    assert release.containerId == cont.id
  }

  public List satisfyContainerRequest(ContainerRequestOperation operation) {
    AMRMClient.ContainerRequest request = operation.request
    Container cont = engine.allocateContainer(request)
    List<Container> allocated = [cont]
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> operations = []
    appState.onContainersAllocated(allocated, assignments, operations)
    return [assignments, cont, request]
  }

  @Test
  public void testComplexAllocation() throws Throwable {
    role0Status.desired = 1
    role1Status.desired = 3

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    List<Container> allocations = engine.execute(ops)
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> releases = []
    appState.onContainersAllocated(allocations, assignments, releases)
    // we expect four release requests here for all the allocated containers
    assertListLength(releases, 4)
    releases.each { assert it instanceof CancelSingleRequest }
    assertListLength(assignments, 4)
    assignments.each { ContainerAssignment assigned ->
      Container target = assigned.container
      RoleInstance ri = roleInstance(assigned)
      appState.containerStartSubmitted(target, ri);
    }
    //insert some async operation here
    assignments.each { ContainerAssignment assigned ->
      Container target = assigned.container
      appState.innerOnNodeManagerContainerStarted(target.id)
    }
    assert engine.containerCount() == 4;
    role1Status.desired = 0
    ops = appState.reviewRequestAndReleaseNodes()
    assertListLength(ops, 3)
    allocations = engine.execute(ops)
    assert engine.containerCount() == 1;

    appState.onContainersAllocated(allocations, assignments, releases)
    assert assignments.empty
    assert releases.empty
  }

  @Test
  public void testDoubleNodeManagerStartEvent() throws Throwable {
    role0Status.desired = 1

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    List<Container> allocations = engine.execute(ops)
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> releases = []
    appState.onContainersAllocated(allocations, assignments, releases)
    assertListLength(assignments, 1)
    ContainerAssignment assigned = assignments[0]
    Container target = assigned.container
    RoleInstance ri = roleInstance(assigned)
    appState.containerStartSubmitted(target, ri);
    RoleInstance ri2 = appState.innerOnNodeManagerContainerStarted(target.id)
    assert ri2 == ri
    //try a second time, expect an error
    try {
      appState.innerOnNodeManagerContainerStarted(target.id)
      fail("Expected an exception")
    } catch (RuntimeException expected) {
      // expected
    }
    //and non-faulter should not downgrade to a null
    log.warn("Ignore any exception/stack trace that appears below")
    log.warn("===============================================================")
    RoleInstance ri3 = appState.onNodeManagerContainerStarted(target.id)
    log.warn("===============================================================")
    log.warn("Ignore any exception/stack trace that appeared above")
    assert ri3 == null
  }

}
