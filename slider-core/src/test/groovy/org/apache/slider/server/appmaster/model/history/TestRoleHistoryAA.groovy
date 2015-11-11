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
    assertResultSize(3, nodeMap.findAllNodesForRole(1, ""))
  }

  @Test
  public void testFindNodesInUnhealthyCluster() throws Throwable {
    // all three will surface at first
    nodeMap.get("one").updateNode(new MockNodeReport("one",NodeState.UNHEALTHY))
    assertResultSize(2, nodeMap.findAllNodesForRole(1, ""))
  }

  @Test
  public void testFindNoNodesWrongLabel() throws Throwable {
    // all three will surface at first
    assertResultSize(0, nodeMap.findAllNodesForRole(1, "GPU"))
  }

  @Test
  public void testFindNoNodesRightLabel() throws Throwable {
    // all three will surface at first
    assertResultSize(3, gpuNodeMap.findAllNodesForRole(1, "GPU"))
  }

  @Test
  public void testFindNoNodesNoLabel() throws Throwable {
    // all three will surface at first
    assertResultSize(3, gpuNodeMap.findAllNodesForRole(1, ""))
  }

  @Test
  public void testFindNoNodesClusterRequested() throws Throwable {
    // all three will surface at first
    applyToNodeEntries(nodeMap) {
      NodeEntry it -> it.request()
    }
    assertResultSize(0, nodeMap.findAllNodesForRole(1, ""))
  }

  @Test
  public void testFindNoNodesClusterBusy() throws Throwable {
    // all three will surface at first
    applyToNodeEntries(nodeMap) {
      NodeEntry it -> it.request()
    }
    assertResultSize(0, nodeMap.findAllNodesForRole(1, ""))
  }

  def assertResultSize(int size, List<NodeInstance> list) {
    if (list.size() != size) {
      list.each { log.error(it.toFullString())}
    }
    assert size == list.size()
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
