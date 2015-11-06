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
import org.apache.slider.providers.PlacementPolicy
import org.apache.slider.providers.ProviderRole
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.state.AppStateBindingInfo
import org.apache.slider.server.appmaster.state.ContainerAssignment
import org.apache.slider.server.appmaster.state.RoleInstance
import org.apache.slider.server.appmaster.state.RoleStatus
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

  RoleStatus aaRole

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

  @Override
  void setup() {
    super.setup()
    aaRole = lookupRole(AAROLE.name)
  }

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
  public void testAllocateAANoLabel() throws Throwable {
    def nodemap = appState.roleHistory.cloneNodemap()
    assert nodemap.size() > 0


    // want multiple instances, so there will be iterations
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

    // verify the pending couner is down
    assert 0L == aaRole.pendingAntiAffineRequests
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

  @Test
  public void testAllocateFlexUp() throws Throwable {
    // want multiple instances, so there will be iterations
    aaRole.desired = 2
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    getSingleRequest(ops)
    assert aaRole.pendingAntiAffineRequests == 1

    // now trigger that flex up
    aaRole.desired = 3

    // expect: no new reqests, pending count ++
    List<AbstractRMOperation> ops2 = appState.reviewRequestAndReleaseNodes()
    assert ops2.empty
    assert aaRole.pendingAntiAffineRequests == 2

    // next iter
    assert 1 == submitOperations(ops, [], ops2).size()
    assert 2 == ops2.size()
    assert aaRole.pendingAntiAffineRequests == 1

    assert 0 == appState.reviewRequestAndReleaseNodes().size()
    // now trigger the next execution cycle
    List<AbstractRMOperation> ops3 = []
    assert 1  == submitOperations(ops2, [], ops3).size()
    assert 2 == ops3.size()
    assert aaRole.pendingAntiAffineRequests == 0
  }

  @Test
  public void testAllocateFlexDown() throws Throwable {
    // want multiple instances, so there will be iterations
    aaRole.desired = 2
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    getSingleRequest(ops)
    assert aaRole.pendingAntiAffineRequests == 1

    // flex down so that the next request should be cancelled
    aaRole.desired = 1

    // expect: no new reqests, pending count --
    List<AbstractRMOperation> ops2 = appState.reviewRequestAndReleaseNodes()
    assert ops2.empty
    assert aaRole.pendingAntiAffineRequests == 0

    // next iter
    submitOperations(ops, [], ops2).size()
    assert 1 == ops2.size()
  }

  /**
   * Here flex down while there is only one outstanding request.
   * The outstanding flex should be cancelled
   * @throws Throwable
   */
  @Test
  public void testAllocateFlexDownForcesCancel() throws Throwable {
    // want multiple instances, so there will be iterations
    aaRole.desired = 1
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    getSingleRequest(ops)
    assert aaRole.pendingAntiAffineRequests == 0

    // flex down so that the next request should be cancelled
    aaRole.desired = 0
    // expect: no new reqests, pending count --
    List<AbstractRMOperation> ops2 = appState.reviewRequestAndReleaseNodes()
    assert aaRole.pendingAntiAffineRequests == 0
    assert ops2.size() == 1
    getSingleCancel(ops2)

    // next iter
    submitOperations(ops, [], ops2).size()
    assert 1 == ops2.size()
  }


}
