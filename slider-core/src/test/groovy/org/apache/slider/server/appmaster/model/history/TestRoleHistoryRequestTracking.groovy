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
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.slider.providers.PlacementPolicy
import org.apache.slider.providers.ProviderRole
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockContainer
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.model.mock.MockRoleHistory
import org.apache.slider.server.appmaster.state.ContainerAllocationOutcome
import org.apache.slider.server.appmaster.state.NodeEntry
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
@Slf4j
@CompileStatic
class TestRoleHistoryRequestTracking extends BaseMockAppStateTest {

  String roleName = "test"

  NodeInstance age1Active4 = nodeInstance(1, 4, 0, 0)
  NodeInstance age2Active2 = nodeInstance(2, 2, 0, 1)
  NodeInstance age2Active0 = nodeInstance(2, 0, 0, 0)
  NodeInstance age3Active0 = nodeInstance(3, 0, 0, 0)
  NodeInstance age4Active1 = nodeInstance(4, 1, 0, 0)
  NodeInstance empty = new NodeInstance("empty", MockFactory.ROLE_COUNT)

  List<NodeInstance> nodes = [age2Active2, age2Active0, age4Active1, age1Active4, age3Active0]
  RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES)
  /** 1MB, 1 vcore*/
  Resource resource = Resource.newInstance(1, 1)

  ProviderRole provRole = new ProviderRole(roleName, 0)
  RoleStatus roleStatus = new RoleStatus(provRole)

  AMRMClient.ContainerRequest requestContainer(RoleStatus roleStatus) {
    roleHistory.requestContainerForRole(roleStatus).issuedRequest
  }

  @Override
  String getTestName() {
    return "TestRoleHistoryAvailableList"
  }

  @Before
  public void setupNodeMap() {
    roleHistory.insert(nodes)
    roleHistory.buildRecentNodeLists();
    roleStatus.setResourceRequirements(Resource.newInstance(1, 1))
  }

  @Test
  public void testAvailableListBuiltForRoles() throws Throwable {
    List<NodeInstance> available0 = roleHistory.cloneRecentNodeList(0)
    assertListEquals([age3Active0, age2Active0], available0)
  }

  @Test
  public void testRequestedNodeOffList() throws Throwable {
    NodeInstance ni = roleHistory.findRecentNodeForNewInstance(roleStatus)
    assert age3Active0 == ni
    assertListEquals([age2Active0], roleHistory.cloneRecentNodeList(0))
    roleHistory.requestInstanceOnNode(ni,
        roleStatus,
        resource
    )
  }

  @Test
  public void testRequestedNodeOffListWithFailures() throws Throwable {
    assert 0 == roleStatus.key
    assert !roleHistory.cloneRecentNodeList(0).empty

    NodeEntry age3role0 = recordAsFailed(age3Active0, 0, 4)
    assert age3Active0.isConsideredUnreliable(0, roleStatus.nodeFailureThreshold)
    recordAsFailed(age2Active0, 0, 4)
    assert age2Active0.isConsideredUnreliable(0, roleStatus.nodeFailureThreshold)
    // expect to get a null node back
    NodeInstance ni = roleHistory.findRecentNodeForNewInstance(roleStatus)
    assert !ni

    // which is translated to a no-location request
    AMRMClient.ContainerRequest req = roleHistory.requestInstanceOnNode(ni,
        roleStatus,
        resource).issuedRequest

    assertNull(req.nodes)

    log.info "resetting failure count"
    age3role0.resetFailedRecently()
    roleHistory.dump()
    assert 0 == age3role0.failedRecently
    assert !age3Active0.isConsideredUnreliable(0, roleStatus.nodeFailureThreshold)
    assert !roleHistory.cloneRecentNodeList(0).empty
    // looking for a node should now find one
    ni = roleHistory.findRecentNodeForNewInstance(roleStatus)
    assert ni == age3Active0
    req = roleHistory.requestInstanceOnNode(ni, roleStatus, resource).issuedRequest
    assert 1 == req.nodes.size()
  }

  /**
   * verify that strict placement policies generate requests for nodes irrespective
   * of their failed status
   * @throws Throwable
   */
  @Test
  public void testStrictPlacementIgnoresFailures() throws Throwable {

    def targetRole = role1Status
    final ProviderRole providerRole1 = targetRole.providerRole
    assert providerRole1.placementPolicy == PlacementPolicy.STRICT
    int key1 = targetRole.key
    def key0 = role0Status.key

    def nodes = [age1Active4, age2Active0, age2Active2, age3Active0, age4Active1]
    recordAllFailed(key0, 4, nodes)
    recordAllFailed(key1, 4, nodes)

    // trigger a list rebuild
    roleHistory.buildRecentNodeLists();
    def recentRole0 = roleHistory.cloneRecentNodeList(key0)
    assert recentRole0.indexOf(age3Active0) < recentRole0.indexOf(age2Active0)

    // the non-strict role has no suitable nodes
    assert null == roleHistory.findRecentNodeForNewInstance(role0Status)


    def ni = roleHistory.findRecentNodeForNewInstance(targetRole)
    assert ni

    def ni2 = roleHistory.findRecentNodeForNewInstance(targetRole)
    assert ni2
    assert ni != ni2
  }

  @Test
  public void testFindAndRequestNode() throws Throwable {
    AMRMClient.ContainerRequest req = requestContainer(roleStatus)

    assert age3Active0.hostname == req.nodes[0]
    List<NodeInstance> a2 = roleHistory.cloneRecentNodeList(0)
    assertListEquals([age2Active0], a2)
  }

  @Test
  public void testRequestedNodeIntoReqList() throws Throwable {
    requestContainer(roleStatus)
    List<OutstandingRequest> requests = roleHistory.listPlacedRequests()
    assert requests.size() == 1
    assert age3Active0.hostname == requests[0].hostname
  }

  @Test
  public void testCompletedRequestDropsNode() throws Throwable {
    AMRMClient.ContainerRequest req = requestContainer(roleStatus)
    List<OutstandingRequest> requests = roleHistory.listPlacedRequests()
    assert requests.size() == 1
    String hostname = requests[0].hostname
    assert age3Active0.hostname == hostname
    assert hostname == req.nodes[0]
    MockContainer container = factory.newContainer(req, hostname)
    assertOnContainerAllocated(container, 2, 1)
    assertNoOutstandingPlacedRequests()
  }

  public void assertOnContainerAllocated(Container c1, int p1, int p2) {
    assert ContainerAllocationOutcome.Open != roleHistory.onContainerAllocated(c1, p1, p2).outcome
  }

  public void assertOnContainerAllocationOpen(Container c1, int p1, int p2) {
    assert ContainerAllocationOutcome.Open == roleHistory.onContainerAllocated(c1, p1, p2).outcome
  }

  def assertNoOutstandingPlacedRequests() {
    assert roleHistory.listPlacedRequests().empty
  }

  public void assertOutstandingPlacedRequests(int i) {
    assert roleHistory.listPlacedRequests().size() == i
  }

  @Test
  public void testTwoRequests() throws Throwable {
    AMRMClient.ContainerRequest req = requestContainer(roleStatus)
    AMRMClient.ContainerRequest req2 = requestContainer(roleStatus)
    List<OutstandingRequest> requests = roleHistory.listPlacedRequests()
    assert requests.size() == 2
    MockContainer container = factory.newContainer(req, req.nodes[0])
    assertOnContainerAllocated(container, 2, 1)
    assertOutstandingPlacedRequests(1)
    container = factory.newContainer(req2, req2.nodes[0])
    assertOnContainerAllocated(container, 2, 2)
    assertNoOutstandingPlacedRequests()
  }

  @Test
  public void testThreeRequestsOneUnsatisified() throws Throwable {
    AMRMClient.ContainerRequest req = requestContainer(roleStatus)
    AMRMClient.ContainerRequest req2 = requestContainer(roleStatus)
    AMRMClient.ContainerRequest req3 = requestContainer(roleStatus)
    List<OutstandingRequest> requests = roleHistory.listPlacedRequests()
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
    assert ContainerAllocationOutcome.Unallocated ==
           roleHistory.onContainerAllocated(container, 3, 3).outcome
    // yet the list is now empty
    assertNoOutstandingPlacedRequests()
    roleHistory.listOpenRequests().empty

    // and the remainder goes onto the available list
    List<NodeInstance> a2 = roleHistory.cloneRecentNodeList(0)
    assertListEquals([age2Active0], a2)
  }

  @Test
  public void testThreeRequests() throws Throwable {
    AMRMClient.ContainerRequest req = requestContainer(roleStatus)
    AMRMClient.ContainerRequest req2 = requestContainer(roleStatus)
    AMRMClient.ContainerRequest req3 = requestContainer(roleStatus)
    assertOutstandingPlacedRequests(2)
    assert req3.nodes == null
    MockContainer container = factory.newContainer(req, req.nodes[0])
    assertOnContainerAllocated(container, 3, 1)
    assertOutstandingPlacedRequests(1)
    container = factory.newContainer(req2, req2.nodes[0])
    assertOnContainerAllocated(container, 3, 2)
    assertNoOutstandingPlacedRequests()
    container = factory.newContainer(req3, "three")
    assertOnContainerAllocationOpen(container, 3, 3)
    assertNoOutstandingPlacedRequests()
  }

}
