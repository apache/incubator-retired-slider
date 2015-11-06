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
import org.apache.hadoop.yarn.api.records.NodeId
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
 * Test Anti-affine placement
 */
@CompileStatic
@Slf4j
class TestMockAppStateAAPlacement extends BaseMockAppStateTest
    implements MockRoles {

//  @Test
  public void testAllocateAA() throws Throwable {

    def aaRole = role2Status

    aaRole.desired = 2

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert 1 == ops.size()
    ContainerRequestOperation operation = (ContainerRequestOperation) ops[0]
    AMRMClient.ContainerRequest request = operation.request
    assert request.relaxLocality
    assert request.nodes == null
    assert request.racks == null
    assert request.capability

    Container allocated = engine.allocateContainer(request)

    // node is allocated wherever

    def firstAllocation = allocated.nodeId

    // notify the container ane expect
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> releaseOperations = []
    appState.onContainersAllocated([allocated], assignments, releaseOperations)

    // verify the release matches the allocation
    assert releaseOperations.size() == 1
    CancelSingleRequest cancelOp = releaseOperations[0] as CancelSingleRequest;
    assert cancelOp.request.capability.equals(allocated.resource)
    // now the assignment
    assert assignments.size() == 1

    // we also expect a new allocation request to have been issued
    //

    ContainerAssignment assigned = assignments[0]
    Container container = assigned.container
    RoleInstance ri = roleInstance(assigned)
    //tell the app it arrived
    appState.containerStartSubmitted(container, ri);
    assert appState.onNodeManagerContainerStarted(container.id)
    ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 0
  }

}
