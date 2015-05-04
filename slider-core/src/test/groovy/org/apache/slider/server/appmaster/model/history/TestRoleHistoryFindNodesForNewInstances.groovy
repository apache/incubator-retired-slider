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

package org.apache.slider.server.appmaster.model.history

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.providers.ProviderRole
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.state.ContainerOutcome
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.RoleHistory
import org.apache.slider.server.appmaster.state.RoleStatus
import org.junit.Before
import org.junit.Test

/**
 * Testing finding nodes for new instances.
 * These tests validate the (currently) suboptimal
 * behavior of not listing any known nodes when there
 * are none in the available list -even if there are nodes
 * known to be not running live instances in the cluster.
 */
@Slf4j
@CompileStatic
class TestRoleHistoryFindNodesForNewInstances extends BaseMockAppStateTest {

  @Override
  String getTestName() {
    return "TestFindNodesForNewInstances"
  }

  NodeInstance age1Active4 = nodeInstance(1, 4, 0, 0)
  NodeInstance age2Active2 = nodeInstance(2, 2, 0, 1)
  NodeInstance age3Active0 = nodeInstance(3, 0, 0, 0)
  NodeInstance age4Active1 = nodeInstance(4, 1, 0, 0)
  NodeInstance age2Active0 = nodeInstance(2, 0, 0, 0)
  NodeInstance empty = new NodeInstance("empty", MockFactory.ROLE_COUNT)

  List<NodeInstance> nodes = [age2Active2, age2Active0, age4Active1, age1Active4, age3Active0]
  RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)

  String roleName = "test"
  RoleStatus roleStat = new RoleStatus(new ProviderRole(roleName, 0))
  RoleStatus roleStat2 = new RoleStatus(new ProviderRole(roleName, 2))
  
  @Before
  public void setupNodeMap() {
    roleHistory.insert(nodes)
    roleHistory.buildAvailableNodeLists();
  }


  public List<NodeInstance> findNodes(int count, RoleStatus roleStatus = roleStat) {
    List < NodeInstance > found = [];
    for (int i = 0; i < count; i++) {
      NodeInstance f = roleHistory.findNodeForNewInstance(roleStatus)
      if (f) {
        found << f
      };
    }
    return found
  }

  @Test
  public void testFind1NodeR0() throws Throwable {
    NodeInstance found = roleHistory.findNodeForNewInstance(roleStat)
    log.info("found: $found")
    assert [age3Active0].contains(found)
  }

  @Test
  public void testFind2NodeR0() throws Throwable {
    NodeInstance found = roleHistory.findNodeForNewInstance(roleStat)
    log.info("found: $found")
    assert [age2Active0, age3Active0].contains(found)
    NodeInstance found2 = roleHistory.findNodeForNewInstance(roleStat)
    log.info("found: $found2")
    assert [age2Active0, age3Active0].contains(found2)
    assert found != found2;
  }

  @Test
  public void testFind3NodeR0ReturnsNull() throws Throwable {
    assert 2== findNodes(2).size()
    NodeInstance found = roleHistory.findNodeForNewInstance(roleStat)
    assert found == null;
  }

  @Test
  public void testFindNodesOneEntry() throws Throwable {
    List<NodeInstance> nodes = findNodes(4, roleStat2)
    assert 0 == nodes.size()
  }

  @Test
  public void testFindNodesIndependent() throws Throwable {
    assert 2 == findNodes(2).size()
    roleHistory.dump()
    assert 0 == findNodes(3, roleStat2).size()
  }

  @Test
  public void testFindNodesFallsBackWhenUsed() throws Throwable {
    // mark age2 and active 0 as busy, expect a null back
    age2Active0.get(0).onStartCompleted()
    assert age2Active0.getActiveRoleInstances(0) != 0
    age3Active0.get(0).onStartCompleted()
    assert age3Active0.getActiveRoleInstances(0) != 0
    NodeInstance found = roleHistory.findNodeForNewInstance(roleStat)
    log.info(found ?.toFullString())
    assert found == null
  }
  @Test
  public void testFindNodesSkipsFailingNode() throws Throwable {
    // mark age2 and active 0 as busy, expect a null back

    def entry0 = age2Active0.get(0)
    entry0.containerCompleted(
        false,
        ContainerOutcome.Failed)
    assert entry0.failed
    assert entry0.failedRecently
    entry0.containerCompleted(
        false,
        ContainerOutcome.Failed)
    assert !age2Active0.exceedsFailureThreshold(roleStat)
    // set failure to 1
    roleStat.providerRole.nodeFailureThreshold = 1
    // threshold is now exceeded
    assert age2Active0.exceedsFailureThreshold(roleStat)

    // get the role & expect age3 to be picked up, even though it is older
    NodeInstance found = roleHistory.findNodeForNewInstance(roleStat)
    assert age3Active0.is(found)
  }

}
