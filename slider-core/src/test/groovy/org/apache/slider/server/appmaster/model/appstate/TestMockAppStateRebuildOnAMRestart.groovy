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
import org.apache.hadoop.yarn.api.records.Container
import org.apache.slider.api.StatusKeys
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockAppState
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.state.MostRecentContainerReleaseSelector
import org.apache.slider.server.appmaster.state.NodeEntry
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.NodeMap
import org.apache.slider.server.appmaster.state.RoleInstance
import org.junit.Test

/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
@CompileStatic
@Slf4j
class TestMockAppStateRebuildOnAMRestart extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  String getTestName() {
    return "TestMockAppStateRebuildOnAMRestart"
  }

  @Test
  public void testRebuild() throws Throwable {

    int r0 = 1
    int r1 = 2
    int r2 = 3
    role0Status.desired = r0
    role1Status.desired = r1
    role2Status.desired = r2
    List<RoleInstance> instances = createAndStartNodes()

    int clusterSize = r0 + r1 + r2
    assert instances.size() == clusterSize

    //clone the list
    List<RoleInstance> cloned = [];
    List<Container> containers = []
    instances.each { RoleInstance elt ->
      cloned.add(elt.clone() as RoleInstance)
      containers.add(elt.container)
    }
    NodeMap nodemap = appState.roleHistory.cloneNodemap()

    // now destroy the app state
    appState = new MockAppState()

    //and rebuild
    appState.buildInstance(
        factory.newInstanceDefinition(r0, r1, r2),
        new Configuration(),
        new Configuration(false),
        factory.ROLES,
        fs,
        historyPath,
        containers,
        null,
        new MostRecentContainerReleaseSelector())

    assert appState.startedCountainerCount == clusterSize

    appState.roleHistory.dump();

    //check that the app state direct structures match
    List<RoleInstance> r0live = appState.enumLiveNodesInRole(ROLE0)
    List<RoleInstance> r1live = appState.enumLiveNodesInRole(ROLE1)
    List<RoleInstance> r2live = appState.enumLiveNodesInRole(ROLE2)

    assert r0 == r0live.size()
    assert r1 == r1live.size()
    assert r2 == r2live.size()

    //now examine the role history
    NodeMap newNodemap = appState.roleHistory.cloneNodemap()

    for (NodeInstance nodeInstance : newNodemap.values()) {
      String hostname = nodeInstance.hostname
      NodeInstance orig = nodemap[hostname]
      assertNotNull("Null entry in original nodemap for " + hostname, orig)

      for (int i = 0; i < ROLE_COUNT; i++) {
        
        assert (nodeInstance.getActiveRoleInstances(i) ==
                orig.getActiveRoleInstances(i))
        NodeEntry origRE = orig.getOrCreate(i)
        NodeEntry newRE = nodeInstance.getOrCreate(i)
        assert origRE.live == newRE.live
        assert newRE.starting == 0
      }
    }
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert ops.size() == 0

    def status = appState.getClusterStatus()
    // verify the AM restart container count was set
    String restarted = status.getInfo(
        StatusKeys.INFO_CONTAINERS_AM_RESTART)
    assert restarted != null;
    //and that the count == 1 master + the region servers
    assert Integer.parseInt(restarted) == containers.size()
  }
}
