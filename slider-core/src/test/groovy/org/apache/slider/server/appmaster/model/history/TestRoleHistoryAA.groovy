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
import org.apache.slider.api.proto.RestTypeMarshalling
import org.apache.slider.api.types.NodeInformationList
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.model.mock.MockRoleHistory
import org.apache.slider.server.appmaster.state.NodeEntry
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.NodeMap
import org.apache.slider.server.appmaster.state.RoleHistory
import org.apache.slider.test.SliderTestBase
import org.junit.Test

/**
 * Test anti-affine
 */
//@CompileStatic
@Slf4j
class TestRoleHistoryAA extends SliderTestBase {

  List<String> hostnames = ["1", "2", "3"]
  NodeMap nodeMap, gpuNodeMap
  RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES)


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
    return setNodeState(nodeMap.get("1"), NodeState.UNHEALTHY)
  }

  protected boolean setNodeState(NodeInstance node, NodeState state) {
    node.updateNode(MockFactory.instance.newNodeReport(node.hostname, state))
  }

  @Test
  public void testFindNoNodesWrongLabel() throws Throwable {
    // all three will surface at first
    verifyResultSize(0, nodeMap.findAllNodesForRole(1, "GPU"))
  }

  @Test
  public void testFindSomeNodesSomeLabel() throws Throwable {
    // all three will surface at first
    update(nodeMap,
      [MockFactory.instance.newNodeReport("1", NodeState.RUNNING, "GPU")])
    def gpuNodes = nodeMap.findAllNodesForRole(1, "GPU")
    verifyResultSize(1, gpuNodes)
    def instance = gpuNodes[0]
    instance.getOrCreate(1).onStarting()
    assert !instance.canHost(1, "GPU")
    assert !instance.canHost(1, "")
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
    def node1 = nodeMap.get("1")
    assert !node1.canHost(1,"")
    node1.get(1).onStartCompleted()
    assert !node1.canHost(1,"")
    assertNoAvailableNodes()
    node1.get(1).release()
    assert node1.canHost(1,"")
    def list2 = verifyResultSize(1, nodeMap.findAllNodesForRole(1, ""))
    assert list2[0].hostname == "1"

    // now tag that node as unhealthy and expect it to go away
    markNodeOneUnhealthy()
    assertNoAvailableNodes()
  }

  @Test
  public void testRolesIndependent() throws Throwable {
    def node1 = nodeMap.get("1")
    def role1 = node1.getOrCreate(1)
    def role2 = node1.getOrCreate(2)
    nodeMap.values().each {
      it.updateNode(MockFactory.instance.newNodeReport("0", NodeState.UNHEALTHY))
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

  @Test
  public void testNodeEntryAvailablity() throws Throwable {
    def entry = new NodeEntry(1)
    assert entry.available
    entry.onStarting()
    assert !entry.available
    entry.onStartCompleted()
    assert !entry.available
    entry.release()
    assert entry.available
    entry.onStarting()
    assert !entry.available
    entry.onStartFailed()
    assert entry.available
  }

  @Test
  public void testNodeInstanceSerialization() throws Throwable {
    def rh2 = new MockRoleHistory([])
    rh2.getOrCreateNodeInstance("localhost")
    def instance = rh2.getOrCreateNodeInstance("localhost")
    instance.getOrCreate(1).onStartCompleted()
    def Map<Integer, String> naming = [(1):"manager"]
    def ni = instance.serialize(naming)
    assert 1 == ni.entries["manager"].live
    def ni2 = rh2.getNodeInformation("localhost", naming)
    assert 1 == ni2.entries["manager"].live
    def info = rh2.getNodeInformationSnapshot(naming)
    assert 1 == info["localhost"].entries["manager"].live
    def nil = new NodeInformationList(info.values());
    assert 1 == nil[0].entries["manager"].live

    def nodeInformationProto = RestTypeMarshalling.marshall(ni)
    def entryProto = nodeInformationProto.getEntries(0)
    assert entryProto && entryProto.getPriority() == 1
    def unmarshalled = RestTypeMarshalling.unmarshall(nodeInformationProto)
    assert unmarshalled.hostname == ni.hostname
    assert unmarshalled.entries.keySet().containsAll(ni.entries.keySet())

  }

  @Test
  public void testBuildRolenames() throws Throwable {

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
    update(nodeMap, nodeReports)
    nodeMap
  }

  protected boolean update(NodeMap nodeMap, List<NodeReport> nodeReports) {
    nodeMap.buildOrUpdate(nodeReports)
  }

  def NodeMap createNodeMap(List<String> hosts, NodeState state,
      String label = "") {
    createNodeMap(MockFactory.instance.createNodeReports(hosts, state, label))
  }
}
