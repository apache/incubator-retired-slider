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
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.model.mock.MockRMOperationHandler
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.state.*
import org.junit.Test

import static org.apache.slider.server.appmaster.state.ContainerPriority.buildPriority
import static org.apache.slider.server.appmaster.state.ContainerPriority.extractRole

@Slf4j
class TestMockRMOperations extends BaseMockAppStateTest implements MockRoles {

  @Override
  String getTestName() {
    return "TestMockRMOperations"
  }

  @Test
  public void testPriorityOnly() throws Throwable {
    assert 5 == buildPriority(5, false)
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
    assert ops.size() == 1
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    int priority = operation.request.priority.priority
    assert extractRole(priority) == MockFactory.PROVIDER_ROLE0.id
    RMOperationHandler handler = new MockRMOperationHandler()
    handler.execute(ops)

    //tell the container its been allocated
    AbstractRMOperation op = handler.operations[0]
    assert op instanceof ContainerRequestOperation
  }

  @Test
  public void testAllocateReleaseOp() throws Throwable {
    role0Status.desired = 1

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    AMRMClient.ContainerRequest request = operation.request
    Container cont = engine.allocateContainer(request)
    List<Container> allocated = [cont]
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> operations = []
    appState.onContainersAllocated(allocated, assignments, operations)
    assert operations.size() == 0
    assert assignments.size() == 1
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
    assert ops.size() == 1

    assert ops[0] instanceof ContainerReleaseOperation
    ContainerReleaseOperation release = (ContainerReleaseOperation) ops[0]
    assert release.containerId == cont.id
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
    assert releases.size() == 0
    assert assignments.size() == 4
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
    assert ops.size() == 3
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
    assert assignments.size() == 1
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

  @Test
  public void testFlexDuringLaunchPhase() throws Throwable {
    role0Status.desired = 1

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    List<Container> allocations = engine.execute(
        ops)
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> releases = []
    appState.onContainersAllocated(allocations, assignments, releases)
    assert assignments.size() == 1
    ContainerAssignment assigned = assignments[0]
    Container target = assigned.container
    RoleInstance ri = roleInstance(assigned)

    ops = appState.reviewRequestAndReleaseNodes()
    assert ops.empty

    //now this is the start point.
    appState.containerStartSubmitted(target, ri);

    ops = appState.reviewRequestAndReleaseNodes()
    assert ops.empty

    RoleInstance ri2 = appState.innerOnNodeManagerContainerStarted(target.id)
  }

  @Test
  public void testFlexBeforeAllocationPhase() throws Throwable {
    role0Status.desired = 1

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert !ops.empty
    List<AbstractRMOperation> ops2 = appState.reviewRequestAndReleaseNodes()
    assert ops2.empty
  }

}
