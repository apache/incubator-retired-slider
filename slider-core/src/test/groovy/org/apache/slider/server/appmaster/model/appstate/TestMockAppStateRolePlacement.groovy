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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.operations.CancelSingleRequest
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation
import org.apache.slider.server.appmaster.state.ContainerAssignment
import org.apache.slider.server.appmaster.state.RoleHistoryUtils
import org.apache.slider.server.appmaster.state.RoleInstance
import org.junit.Test

import static org.apache.slider.server.appmaster.state.ContainerPriority.extractRole

/**
 * Test that the app state lets you ask for nodes, get a specific host,
 * release it and then get that one back again.
 */
@CompileStatic
@Slf4j
class TestMockAppStateRolePlacement extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  String getTestName() {
    return "TestMockAppStateRolePlacement"
  }


  @Test
  public void testAllocateReleaseRealloc() throws Throwable {
    role0Status.desired = 1

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    AMRMClient.ContainerRequest request = operation.request
    assert request.relaxLocality
    assert request.nodes == null
    assert request.racks == null
    assert request.capability

    Container allocated = engine.allocateContainer(request)
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> releaseOperations = []
    appState.onContainersAllocated([(Container)allocated], assignments, releaseOperations)
    // verify the release matches the allocation
    assert releaseOperations.size() == 1
    CancelSingleRequest cancelOp = releaseOperations[0] as CancelSingleRequest;
    assert cancelOp.request
    assert cancelOp.request.capability
    assert cancelOp.request.capability.equals(allocated.resource)
    // now the assignment
    assert assignments.size() == 1
    ContainerAssignment assigned = assignments[0]
    Container container = assigned.container
    assert container.id == allocated.id
    int roleId = assigned.role.priority
    assert roleId == extractRole(request.priority)
    assert assigned.role.name == ROLE0
    String containerHostname = RoleHistoryUtils.hostnameOf(container);
    RoleInstance ri = roleInstance(assigned)
    //tell the app it arrived
    appState.containerStartSubmitted(container, ri);
    assert appState.onNodeManagerContainerStarted(container.id)
    assert role0Status.started == 1
    ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 0

    //now it is surplus
    role0Status.desired = 0
    ops = appState.reviewRequestAndReleaseNodes()
    ContainerReleaseOperation release = (ContainerReleaseOperation) ops[0]
    
    assert release.containerId == container.id
    engine.execute(ops)
    assert appState.onCompletedNode(containerStatus(container)).roleInstance 

    //view the world
    appState.getRoleHistory().dump();
    
    //now ask for a new one
    role0Status.desired = 1
    ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 1
    operation = (ContainerRequestOperation) ops[0]
    AMRMClient.ContainerRequest request2 = operation.request
    assert request2 != null
    assert request2.nodes[0] == containerHostname
    assert !request2.relaxLocality
    engine.execute(ops)

  }

}
