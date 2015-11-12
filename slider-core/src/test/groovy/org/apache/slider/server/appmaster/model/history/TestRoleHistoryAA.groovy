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

import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.NodeReport
import org.apache.hadoop.yarn.api.records.NodeState
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.model.mock.MockNodeReport
import org.apache.slider.server.appmaster.model.mock.MockRoleHistory
import org.apache.slider.server.appmaster.state.NodeEntry
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.NodeMap
import org.apache.slider.server.appmaster.state.RoleHistory
import org.apache.slider.server.appmaster.state.RoleStatus
import org.apache.slider.test.SliderTestBase
import org.junit.Test

/**
 * Test anti-affine
 */
//@CompileStatic
@Slf4j
class TestRoleHistoryAA extends SliderTestBase {

  List<String> hostnames = ["one", "two", "three"]
  NodeMap nodeMap, gpuNodeMap
  RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES)

  AMRMClient.ContainerRequest requestContainer(RoleStatus roleStatus) {
    roleHistory.requestContainerForRole(roleStatus).issuedRequest
  }

  @Override
  void setup() {
    super.setup()
    nodeMap = createNodeMap(hostnames, NodeState.RUNNING)
    gpuNodeMap = createNodeMap(hostnames, NodeState.RUNNING, "GPU")
  }

  @Test
  public void testFindNodesInFullCluster() throws Throwable {
    // all three will surface at first
    verifyResultSize(3, nodeMap.findAllNodesForRole(1, ""))
  }

  @Test
  public void testFindNodesInUnhealthyCluster() throws Throwable {
    // all three will surface at first
    markNodeOneUnhealthy()
    verifyResultSize(2, nodeMap.findAllNodesForRole(1, ""))
  }

  public boolean markNodeOneUnhealthy() {
    return setNodeState(nodeMap.get("one"), NodeState.UNHEALTHY)
  }

  protected boolean setNodeState(NodeInstance node, NodeState state) {
    node.updateNode(new  MockNodeReport(node.hostname, state))
  }

  @Test
  public void testFindNoNodesWrongLabel() throws Throwable {
    // all three will surface at first
    verifyResultSize(0, nodeMap.findAllNodesForRole(1, "GPU"))
  }

  @Test
  public void testFindNoNodesRightLabel() throws Throwable {
    // all three will surface at first
    verifyResultSize(3, gpuNodeMap.findAllNodesForRole(1, "GPU"))
  }

  @Test
  public void testFindNoNodesNoLabel() throws Throwable {
    // all three will surface at first
    verifyResultSize(3, gpuNodeMap.findAllNodesForRole(1, ""))
  }

  @Test
  public void testFindNoNodesClusterRequested() throws Throwable {
    // all three will surface at first
    applyToNodeEntries(nodeMap) {
      NodeEntry it -> it.request()
    }
    assertNoAvailableNodes(1)
  }

  @Test
  public void testFindNoNodesClusterBusy() throws Throwable {
    // all three will surface at first
    applyToNodeEntries(nodeMap) {
      NodeEntry it -> it.request()
    }
    assertNoAvailableNodes(1)
  }

  /**
   * Tag all nodes as starting, then walk one through a bit
   * more of its lifecycle
   */
  @Test
  public void testFindNoNodesLifecycle() throws Throwable {
    // all three will surface at first
    applyToNodeEntries(nodeMap) {
      NodeEntry it -> it.onStarting()
    }
    assertNoAvailableNodes(1)

    // walk one of the nodes through the lifecycle
    def node1 = nodeMap.get("one")
    assert !node1.canHost(1,"")
    node1.get(1).onStartCompleted()
    assert !node1.canHost(1,"")
    assertNoAvailableNodes()
    node1.get(1).release()
    assert node1.canHost(1,"")
    def list2 = verifyResultSize(1, nodeMap.findAllNodesForRole(1, ""))
    assert list2[0].hostname == "one"

    // now tag that node as unhealthy and expect it to go away
    markNodeOneUnhealthy()
    assertNoAvailableNodes()
  }

  @Test
  public void testRolesIndependent() throws Throwable {
    def node1 = nodeMap.get("one")
    def role1 = node1.getOrCreate(1)
    def role2 = node1.getOrCreate(2)
    nodeMap.values().each {
      it.updateNode(new MockNodeReport("", NodeState.UNHEALTHY))
    }
    assertNoAvailableNodes(1)
    assertNoAvailableNodes(2)
    assert setNodeState(node1, NodeState.RUNNING)
    // tag role 1 as busy
    role1.onStarting()
    assertNoAvailableNodes(1)

    verifyResultSize(1, nodeMap.findAllNodesForRole(2, ""))
    assert node1.canHost(2,"")
  }


  public List<NodeInstance> assertNoAvailableNodes(int role = 1, String label = "") {
    return verifyResultSize(0, nodeMap.findAllNodesForRole(role, label))
  }

  List<NodeInstance> verifyResultSize(int size, List<NodeInstance> list) {
    if (list.size() != size) {
      list.each { log.error(it.toFullString()) }
    }
    assert size == list.size()
    list
  }

  def applyToNodeEntries(Collection<NodeInstance> list, Closure cl) {
    list.each { it -> cl(it.getOrCreate(1)) }
  }

  def applyToNodeEntries(NodeMap nodeMap, Closure cl) {
    applyToNodeEntries(nodeMap.values(), cl)
  }

  def NodeMap createNodeMap(List<NodeReport> nodeReports) {
    NodeMap nodeMap = new NodeMap(1)
    nodeMap.buildOrUpdate(nodeReports)
    nodeMap
  }

  def NodeMap createNodeMap(List<String> hosts, NodeState state,
      String label = "") {
    createNodeMap(MockNodeReport.createInstances(hosts, state, label))
  }
}
