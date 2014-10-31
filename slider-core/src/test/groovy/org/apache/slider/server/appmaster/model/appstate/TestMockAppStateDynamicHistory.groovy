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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.slider.api.ResourceKeys
import org.apache.slider.core.conf.ConfTreeOperations
import org.apache.slider.providers.PlacementPolicy
import org.apache.slider.providers.ProviderRole
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockAppState
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation
import org.apache.slider.server.appmaster.state.AppState
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.RoleInstance
import org.apache.slider.server.appmaster.state.SimpleReleaseSelector
import org.junit.Test

/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
@CompileStatic
@Slf4j
class TestMockAppStateDynamicHistory extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  String getTestName() {
    return "TestMockAppStateDynamicHistory"
  }

  /**
   * Small cluster with multiple containers per node,
   * to guarantee many container allocations on each node
   * @return
   */
  @Override
  MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(8, 1)
  }

  @Override
  void initApp() {
    super.initApp()
    appState = new MockAppState()
    appState.setContainerLimits(RM_MAX_RAM, RM_MAX_CORES)

    def instance = factory.newInstanceDefinition(0,0,0)

    appState.buildInstance(
        instance,
        new Configuration(),
        new Configuration(false),
        factory.ROLES,
        fs,
        historyPath,
        null,
        null, new SimpleReleaseSelector())
  }


  @Test
  public void testDynamicRoleHistory() throws Throwable {

    def dynamic = "dynamicRole"
    int priority_num_8 = 8
    int desired = 1
    int placementPolicy = 0
    // snapshot and patch existing spec
    def resources = ConfTreeOperations.fromInstance(
        appState.resourcesSnapshot.confTree)
    def opts = [
        (ResourceKeys.COMPONENT_INSTANCES): ""+desired,
        (ResourceKeys.COMPONENT_PRIORITY) : "" +priority_num_8,
        (ResourceKeys.COMPONENT_PLACEMENT_POLICY): "" + placementPolicy
    ]

    resources.components[dynamic] = opts


    // write the definitions
    def updates = appState.updateResourceDefinitions(resources.confTree);
    assert updates.size() == 1
    def updatedRole = updates[0]
    assert updatedRole.placementPolicy == placementPolicy

    // verify the new role was persisted
    def snapshotDefinition = appState.resourcesSnapshot.getMandatoryComponent(
        dynamic)
    assert snapshotDefinition.getMandatoryOptionInt(
        ResourceKeys.COMPONENT_PRIORITY) == priority_num_8

    // now look at the role map
    assert appState.roleMap[dynamic] != null
    def mappedRole = appState.roleMap[dynamic]
    assert mappedRole.id == priority_num_8

    def priorityMap = appState.rolePriorityMap
    assert priorityMap.size() == 4
    ProviderRole dynamicProviderRole
    assert (dynamicProviderRole = priorityMap[priority_num_8]) != null
    assert dynamicProviderRole.id == priority_num_8

    assert null != appState.roleStatusMap[priority_num_8]
    def dynamicRoleStatus = appState.roleStatusMap[priority_num_8]
    assert dynamicRoleStatus.desired == desired

    
    // before allocating the nodes, fill up the capacity of some of the
    // hosts
    engine.allocator.nextIndex()

    def targetNode = 2
    assert targetNode == engine.allocator.nextIndex()
    def targetHostname = engine.cluster.nodeAt(targetNode).hostname

    // allocate the nodes
    def actions = appState.reviewRequestAndReleaseNodes()
    assert actions.size() == 1
    def action0 = (ContainerRequestOperation)actions[0]

    def request = action0.request
    assert !request.nodes

    List<ContainerId> released = []
    List<RoleInstance> allocations = submitOperations(actions, released)
    processSubmissionOperations(allocations, [], released)
    assert allocations.size() == 1
    RoleInstance ri = allocations[0]
    
    assert ri.role == dynamic
    assert ri.roleId == priority_num_8
    assert ri.host.host == targetHostname

    // now look at the role history

    def roleHistory = appState.roleHistory
    def activeNodes = roleHistory.listActiveNodes(priority_num_8)
    assert activeNodes.size() == 1
    NodeInstance activeNode = activeNodes[0]

    assert activeNode.hostname == targetHostname
    
    // now trigger a termination event on that role


    def cid = ri.id
    // failure
    AppState.NodeCompletionResult result = appState.onCompletedNode(
        containerStatus(cid, 1))
    assert result.roleInstance == ri
    assert result.containerFailed

    def nodeForNewInstance = roleHistory.findNodeForNewInstance(
        dynamicRoleStatus)
    assert nodeForNewInstance
    
    // make sure new nodes will default to a different host in the engine
    assert targetNode < engine.allocator.nextIndex()

    actions = appState.reviewRequestAndReleaseNodes()
    assert actions.size() == 1
    def action1 = (ContainerRequestOperation) actions[0]
    def request1 = action1.request
    assert request1.nodes
  }
}
