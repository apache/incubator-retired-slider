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
import org.apache.slider.providers.PlacementPolicy
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockAppState
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation
import org.apache.slider.server.appmaster.state.AppState
import org.apache.slider.server.appmaster.state.ContainerPriority
import org.apache.slider.server.appmaster.state.RoleHistoryUtils
import org.apache.slider.server.appmaster.state.RoleInstance
import org.apache.slider.server.appmaster.state.SimpleReleaseSelector
import org.junit.Test

/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
@CompileStatic
@Slf4j
class TestMockAppStateDynamicRoles extends BaseMockAppStateTest
    implements MockRoles {
  private static final String ROLE4 = "4"
  private static final String ROLE5 = "5"
  private static final int ID4 = 4
  private static final int ID5 = 5

  @Override
  String getTestName() {
    return "TestMockAppStateDynamicRoles"
  }

  /**
   * Small cluster with multiple containers per node,
   * to guarantee many container allocations on each node
   * @return
   */
  @Override
  MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(8, 2)
  }

  @Override
  void initApp() {
    super.initApp()
    appState = new MockAppState()
    appState.setContainerLimits(RM_MAX_RAM, RM_MAX_CORES)
    def instance = factory.newInstanceDefinition(0,0,0)

    def opts = [
        (ResourceKeys.COMPONENT_PRIORITY): ROLE4,
        (ResourceKeys.COMPONENT_INSTANCES): "1",
    ]


    instance.resourceOperations.components[ROLE4]= opts

    def opts5 = [
        (ResourceKeys.COMPONENT_PRIORITY) : ROLE5,
        (ResourceKeys.COMPONENT_INSTANCES): "1",
        (ResourceKeys.COMPONENT_PLACEMENT_POLICY):
            Integer.toString(PlacementPolicy.STRICT),
        (ResourceKeys.NODE_FAILURE_THRESHOLD):
            Integer.toString(2),
    ]

    instance.resourceOperations.components[ROLE5]= opts5

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
  public void testAllocateReleaseRealloc() throws Throwable {

    createAndStartNodes()
    appState.reviewRequestAndReleaseNodes()
    appState.getRoleHistory().dump();
  }

  /**
   * Find all allocations for a specific role
   * @param role role Id/priority
   * @param actions source list
   * @return found list
   */
  List<ContainerRequestOperation> findAllocationsForRole(int role, 
      List<AbstractRMOperation> actions) {
    List <ContainerRequestOperation > results = []
    actions.each { AbstractRMOperation  operation ->
      if (operation instanceof ContainerRequestOperation) {
        def req = (ContainerRequestOperation) operation;
        def reqrole = ContainerPriority.extractRole(req.request.priority)
        if (role == reqrole) {
          results << req
        }
      }
    }
    return results
  } 
  
  @Test
  public void testStrictPlacementInitialRequest() throws Throwable {
    log.info("Initial engine state = $engine")
    List<AbstractRMOperation> actions = appState.reviewRequestAndReleaseNodes()
    assert actions.size() == 2

    // neither have locality at this point
    assertRelaxLocalityFlag(ID4, null, true, actions)
    assertRelaxLocalityFlag(ID5, null, true, actions)
  }


  @Test
  public void testPolicyPropagation() throws Throwable {
    assert !(appState.lookupRoleStatus(ROLE4).placementPolicy & PlacementPolicy.STRICT)
    assert (appState.lookupRoleStatus(ROLE5).placementPolicy & PlacementPolicy.STRICT)

  }

  @Test
  public void testNodeFailureThresholdPropagation() throws Throwable {
    assert (appState.lookupRoleStatus(ROLE4).nodeFailureThreshold == 3)
    assert (appState.lookupRoleStatus(ROLE5).nodeFailureThreshold == 2)

  }

  @Test
  public void testLaxPlacementSecondRequestRole4() throws Throwable {
    log.info("Initial engine state = $engine")
    def role4 = appState.lookupRoleStatus(ROLE4)
    def role5 = appState.lookupRoleStatus(ROLE5)
    role4.desired = 1
    role5.desired = 0

    def instances = createStartAndStopNodes([])
    assert instances.size() == 1

    def instanceA = instances.find { RoleInstance instance ->
      instance.roleId == ID4
    }
    assert instanceA
    def hostname = RoleHistoryUtils.hostnameOf(instanceA.container)


    log.info("Allocated engine state = $engine")
    assert engine.containerCount() == 1

    assert role4.actual == 1
    // shrinking cluster

    role4.desired = 0
    appState.lookupRoleStatus(ROLE4).desired = 0
    def completionResults = []
    def containersToRelease = []
    instances = createStartAndStopNodes(completionResults)
    assert engine.containerCount() == 0
    assert completionResults.size() == 1

    // expanding: expect hostnames  now
    role4.desired = 1
    def actions = appState.reviewRequestAndReleaseNodes()
    assert actions.size() == 1

    ContainerRequestOperation cro = (ContainerRequestOperation) actions[0]
    def nodes = cro.request.nodes
    assert nodes.size() == 1
    assert hostname == nodes[0]
  }

  @Test
  public void testStrictPlacementSecondRequestRole5() throws Throwable {
    log.info("Initial engine state = $engine")
    def role4 = appState.lookupRoleStatus(ROLE4)
    def role5 = appState.lookupRoleStatus(ROLE5)
    role4.desired = 0
    role5.desired = 1

    def instances = createStartAndStopNodes([])
    assert instances.size() == 1

    def instanceA = instances.find { RoleInstance instance ->
      instance.roleId = ID5
    }
    assert instanceA
    def hostname = RoleHistoryUtils.hostnameOf(instanceA.container)
    


    log.info("Allocated engine state = $engine")
    assert engine.containerCount() == 1

    assert role5.actual == 1
    // shrinking cluster

    role5.desired = 0
    def completionResults = []
    def containersToRelease = []
    instances = createStartAndStopNodes(completionResults)
    assert engine.containerCount() == 0
    assert completionResults.size() == 1
    assert role5.actual == 0

    role5.desired = 1
    def actions = appState.reviewRequestAndReleaseNodes()
    assert actions.size() == 1
    assertRelaxLocalityFlag(ID5, "", false, actions)
    ContainerRequestOperation cro = (ContainerRequestOperation) actions[0]
    def nodes = cro.request.nodes
    assert nodes.size() == 1
    assert hostname == nodes[0]
    
  }

  public void assertRelaxLocalityFlag(
      int id,
      String expectedHost,
      boolean expectedRelaxFlag,
      List<AbstractRMOperation> actions) {
    def requests
    requests = findAllocationsForRole(id, actions)
    assert requests.size() == 1
    def req = requests[0]
    assert expectedRelaxFlag == req.request.relaxLocality
  }

}
