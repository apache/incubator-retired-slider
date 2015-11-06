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
import org.apache.slider.api.ResourceKeys
import org.apache.slider.providers.PlacementPolicy
import org.apache.slider.providers.ProviderRole
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.operations.CancelSingleRequest
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation
import org.apache.slider.server.appmaster.state.AppStateBindingInfo
import org.apache.slider.server.appmaster.state.ContainerAssignment
import org.apache.slider.server.appmaster.state.NodeMap
import org.apache.slider.server.appmaster.state.RoleInstance
import org.junit.Test

/**
 * Test Anti-affine placement
 */
@CompileStatic
@Slf4j
class TestMockAppStateAAPlacement extends BaseMockAppStateTest
    implements MockRoles {

  /**
   * Patch up a "role2" role to have anti-affinity set
   */
  public static final ProviderRole AAROLE = new ProviderRole(
      MockRoles.ROLE2,
      2,
      PlacementPolicy.ANTI_AFFINITY_REQUIRED,
      2,
      2,
      null)

  @Override
  AppStateBindingInfo buildBindingInfo() {
    def bindingInfo = super.buildBindingInfo()
    bindingInfo.roles = [
        MockFactory.PROVIDER_ROLE0,
        MockFactory.PROVIDER_ROLE1,
        AAROLE,
    ]
    bindingInfo
  }

  private static final int roleId = AAROLE.id

  /**
   * Get the single request of a list of operations; includes the check for the size
   * @param ops operations list of size 1
   * @return the request within the first operation
   */
  public AMRMClient.ContainerRequest getSingleCancel(List<AbstractRMOperation> ops) {
    assert 1 == ops.size()
    getCancel(ops, 0)
  }

  @Test
  public void testVerifyNodeMap() throws Throwable {

    def nodemap = appState.roleHistory.cloneNodemap()
    assert nodemap.size() > 0
  }

  @Test
  public void testAllocateAANoLabel() throws Throwable {

    def aaRole = lookupRole(AAROLE.name)

    // want two instances, so there will be two iterations
    aaRole.desired = 2

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    AMRMClient.ContainerRequest request = getSingleRequest(ops)
    assert request.relaxLocality
    assert request.nodes == null
    assert request.racks == null
    assert request.capability

    Container allocated = engine.allocateContainer(request)

    // notify the container ane expect
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> operations = []
    appState.onContainersAllocated([allocated], assignments, operations)

    // assignment
    assert assignments.size() == 1

    // verify the release matches the allocation
    assert operations.size() == 2
    assert getCancel(operations, 0).capability.equals(allocated.resource)

    // we also expect a new allocation request to have been issued

    def req2 = getRequest(operations, 1)
    Container allocated2 = engine.allocateContainer(req2)

    // placement must be on a different host
    assert allocated2.nodeId != allocated.nodeId

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
