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

import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.slider.providers.ProviderRole
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockContainer
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.state.ContainerAllocationOutcome
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.OutstandingRequest
import org.apache.slider.server.appmaster.state.RoleHistory
import org.apache.slider.server.appmaster.state.RoleStatus
import org.junit.Before
import org.junit.Test

/**
 * Test the RH availability list and request tracking: that hosts
 * get removed and added 
 */
class TestRoleHistoryRequestTracking extends BaseMockAppStateTest {

  String roleName = "test"

  NodeInstance age1Active4 = nodeInstance(1, 4, 0, 0)
  NodeInstance age2Active2 = nodeInstance(2, 2, 0, 1)
  NodeInstance age3Active0 = nodeInstance(3, 0, 0, 0)
  NodeInstance age4Active1 = nodeInstance(4, 1, 0, 0)
  NodeInstance age2Active0 = nodeInstance(2, 0, 0, 0)
  NodeInstance empty = new NodeInstance("empty", MockFactory.ROLE_COUNT)

  List<NodeInstance> nodes = [age2Active2, age2Active0, age4Active1, age1Active4, age3Active0]
  RoleHistory roleHistory = new RoleHistory(MockFactory.ROLES)
  Resource resource = Resource.newInstance(1, 1)

  ProviderRole provRole = new ProviderRole(roleName, 0)
  RoleStatus roleStatus = new RoleStatus(provRole)

  @Override
  String getTestName() {
    return "TestRoleHistoryAvailableList"
  }

  @Before
  public void setupNodeMap() {
    roleHistory.insert(nodes)
    roleHistory.buildAvailableNodeLists();
  }

  @Test
  public void testAvailableListBuiltForRoles() throws Throwable {
    List<NodeInstance> available0 = roleHistory.cloneAvailableList(0)
    assertListEquals([age3Active0, age2Active0], available0)
  }

  @Test
  public void testRequestedNodeOffList() throws Throwable {
    List<NodeInstance> available0 = roleHistory.cloneAvailableList(0)
    NodeInstance ni = roleHistory.findNodeForNewInstance(roleStatus)
    assert age3Active0 == ni
    AMRMClient.ContainerRequest req = roleHistory.requestInstanceOnNode(ni,
        roleStatus,
        resource,
        "")
    List<NodeInstance> a2 = roleHistory.cloneAvailableList(0)
    assertListEquals([age2Active0], a2)
  }

  @Test
  public void testRequestedNodeOffListWithFailures() throws Throwable {
    NodeInstance ni = roleHistory.findNodeForNewInstance(roleStatus)
    assert age3Active0 == ni
    AMRMClient.ContainerRequest req = roleHistory.requestInstanceOnNode(ni,
        roleStatus,
        resource,
        "")
    assert 1 == req.nodes.size()
    List<NodeInstance> a2 = roleHistory.cloneAvailableList(0)
    assertListEquals([age2Active0], a2)

    age3Active0.get(0).failedRecently = 4
    req = roleHistory.requestInstanceOnNode(ni,
        roleStatus,
        resource,
        "")
    assertNull(req.nodes)

    age3Active0.get(0).failedRecently = 0
    req = roleHistory.requestInstanceOnNode(ni,
        roleStatus,
        resource,
        "")
    assert 1 == req.nodes.size()
  }

  @Test
  public void testFindAndRequestNode() throws Throwable {
    AMRMClient.ContainerRequest req = roleHistory.requestNode(roleStatus, resource)

    assert age3Active0.hostname == req.nodes[0]
    List<NodeInstance> a2 = roleHistory.cloneAvailableList(0)
    assertListEquals([age2Active0], a2)
  }

  @Test
  public void testRequestedNodeIntoReqList() throws Throwable {
    AMRMClient.ContainerRequest req = roleHistory.requestNode(roleStatus, resource)
    List<OutstandingRequest> requests = roleHistory.listOutstandingPlacedRequests()
    assert requests.size() == 1
    assert age3Active0.hostname == requests[0].hostname
  }

  @Test
  public void testCompletedRequestDropsNode() throws Throwable {
    AMRMClient.ContainerRequest req = roleHistory.requestNode(roleStatus, resource)
    List<OutstandingRequest> requests = roleHistory.listOutstandingPlacedRequests()
    assert requests.size() == 1
    String hostname = requests[0].hostname
    assert age3Active0.hostname == hostname
    assert hostname == req.nodes[0]
    MockContainer container = factory.newContainer(req, hostname)
    assertOnContainerAllocated(container, 2, 1)
    assertNoOutstandingRequests()
  }

  public void assertOnContainerAllocated(MockContainer c1, int p1, int p2) {
    assert ContainerAllocationOutcome.Open != roleHistory.onContainerAllocated(c1, p1, p2)
  }

  public void assertOnContainerAllocationOpen(MockContainer c1, int p1, int p2) {
    assert ContainerAllocationOutcome.Open == roleHistory.onContainerAllocated(c1, p1, p2)
  }

  def assertNoOutstandingRequests() {
    assert roleHistory.listOutstandingPlacedRequests().empty
  }

  public void assertOutstandingPlacedRequests(int i) {
    assert roleHistory.listOutstandingPlacedRequests().size() == i
  }

  @Test
  public void testTwoRequests() throws Throwable {
    AMRMClient.ContainerRequest req = roleHistory.requestNode(roleStatus, resource)
    AMRMClient.ContainerRequest req2 = roleHistory.requestNode(roleStatus, resource)
    List<OutstandingRequest> requests = roleHistory.listOutstandingPlacedRequests()
    assert requests.size() == 2
    MockContainer container = factory.newContainer(req, req.nodes[0])
    assertOnContainerAllocated(container, 2, 1)
    assertOutstandingPlacedRequests(1)
    container = factory.newContainer(req2, req2.nodes[0])
    assertOnContainerAllocated(container, 2, 2)
    assertNoOutstandingRequests()
  }

  @Test
  public void testThreeRequestsOneUnsatisified() throws Throwable {
    AMRMClient.ContainerRequest req = roleHistory.requestNode(roleStatus, resource)
    AMRMClient.ContainerRequest req2 = roleHistory.requestNode(roleStatus, resource)
    AMRMClient.ContainerRequest req3 = roleHistory.requestNode(roleStatus, resource)
    List<OutstandingRequest> requests = roleHistory.listOutstandingPlacedRequests()
    assert requests.size() == 2
    MockContainer container = factory.newContainer(req, req.nodes[0])
    assertOnContainerAllocated(container, 2, 1)
    assertOutstandingPlacedRequests(1)

    container = factory.newContainer(req3, "three")
    assertOnContainerAllocationOpen(container, 3, 2)
    assertOutstandingPlacedRequests(1)

    // the final allocation will trigger a cleanup
    container = factory.newContainer(req2, "four")
    // no node dropped
    assertOnContainerAllocationOpen(container, 3, 3)
    // yet the list is now empty
    assertNoOutstandingRequests()

    // and the remainder goes onto the available list
    List<NodeInstance> a2 = roleHistory.cloneAvailableList(0)
    assertListEquals([age2Active0], a2)

  }

  @Test
  public void testThreeRequests() throws Throwable {
    AMRMClient.ContainerRequest req = roleHistory.requestNode(roleStatus, resource)
    AMRMClient.ContainerRequest req2 = roleHistory.requestNode(roleStatus, resource)
    AMRMClient.ContainerRequest req3 = roleHistory.requestNode(roleStatus, resource)
    assertOutstandingPlacedRequests(2)
    assert req3.nodes == null
    MockContainer container = factory.newContainer(req, req.nodes[0])
    assertOnContainerAllocated(container, 3, 1)
    assertOutstandingPlacedRequests(1)
    container = factory.newContainer(req2, req2.nodes[0])
    assertOnContainerAllocated(container, 3, 2)
    assertNoOutstandingRequests()
    container = factory.newContainer(req3, "three")
    assertOnContainerAllocationOpen(container, 3, 3)
    assertNoOutstandingRequests()
  }

}
