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
                                                                        resource)
    List<NodeInstance> a2 = roleHistory.cloneAvailableList(0)
    assertListEquals([age2Active0], a2)
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
    List<OutstandingRequest> requests = roleHistory.outstandingRequestList
    assert requests.size() == 1
    assert age3Active0.hostname == requests[0].hostname
  }
  
  @Test
  public void testCompletedRequestDropsNode() throws Throwable {
    AMRMClient.ContainerRequest req = roleHistory.requestNode(roleStatus, resource)
    List<OutstandingRequest> requests = roleHistory.outstandingRequestList
    assert requests.size() == 1
    String hostname = requests[0].hostname
    assert age3Active0.hostname == hostname
    assert hostname == req.nodes[0]
    MockContainer container = factory.newContainer(req, hostname)
    assert roleHistory.onContainerAllocated(container , 2, 1)
    assert roleHistory.outstandingRequestList.empty
  }
  
  @Test
  public void testTwoRequests() throws Throwable {
    AMRMClient.ContainerRequest req = roleHistory.requestNode(roleStatus, resource)
    AMRMClient.ContainerRequest req2 = roleHistory.requestNode(roleStatus, resource)
    List<OutstandingRequest> requests = roleHistory.outstandingRequestList
    assert requests.size() == 2
    MockContainer container = factory.newContainer(req, req.nodes[0])
    assert roleHistory.onContainerAllocated(container , 2, 1)
    assert roleHistory.outstandingRequestList.size() == 1
    container = factory.newContainer(req2, req2.nodes[0])
    assert roleHistory.onContainerAllocated(container, 2, 2)
    assert roleHistory.outstandingRequestList.empty
  }

    
  @Test
  public void testThreeRequestsOneUnsatisified() throws Throwable {
    AMRMClient.ContainerRequest req = roleHistory.requestNode(roleStatus, resource)
    AMRMClient.ContainerRequest req2 = roleHistory.requestNode(roleStatus, resource)
    AMRMClient.ContainerRequest req3 = roleHistory.requestNode(roleStatus, resource)
    List<OutstandingRequest> requests = roleHistory.outstandingRequestList
    assert requests.size() == 2
    MockContainer container = factory.newContainer(req, req.nodes[0])
    assert roleHistory.onContainerAllocated(container , 2, 1)
    assert roleHistory.outstandingRequestList.size() == 1
    
    container = factory.newContainer(req3, "three")
    assert !roleHistory.onContainerAllocated(container, 3, 2)
    assert roleHistory.outstandingRequestList.size() == 1
    
    // the final allocation will trigger a cleanup
    container = factory.newContainer(req2, "four")
    // no node dropped
    assert !roleHistory.onContainerAllocated(container, 3, 3)
    // yet the list is now empty
    assert roleHistory.outstandingRequestList.empty

    // and the remainder goes onto the available list
    List<NodeInstance> a2 = roleHistory.cloneAvailableList(0)
    assertListEquals([age2Active0], a2)

  }

  
  @Test
  public void testThreeRequests() throws Throwable {
    AMRMClient.ContainerRequest req = roleHistory.requestNode(roleStatus, resource)
    AMRMClient.ContainerRequest req2 = roleHistory.requestNode(roleStatus, resource)
    AMRMClient.ContainerRequest req3 = roleHistory.requestNode(roleStatus, resource)
    assert roleHistory.outstandingRequestList.size() == 2
    assert req3.nodes == null
    MockContainer container = factory.newContainer(req, req.nodes[0])
    assert roleHistory.onContainerAllocated(container , 3, 1)
    assert roleHistory.outstandingRequestList.size() == 1
    container = factory.newContainer(req2, req2.nodes[0])
    assert roleHistory.onContainerAllocated(container, 3, 2)
    assert roleHistory.outstandingRequestList.empty
    container = factory.newContainer(req3, "three")
    assert !roleHistory.onContainerAllocated(container, 3, 3)
    assert roleHistory.outstandingRequestList.empty
  }

}
