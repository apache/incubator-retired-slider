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
import org.apache.slider.providers.PlacementPolicy
import org.apache.slider.providers.ProviderRole
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockResource
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.operations.CancelSingleRequest
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.OutstandingRequest
import org.apache.slider.server.appmaster.state.OutstandingRequestTracker
import org.junit.Test

class TestRoleHistoryOutstandingRequestTracker extends BaseMockAppStateTest {

  NodeInstance host1 = new NodeInstance("host1", 3)
  NodeInstance host2 = new NodeInstance("host2", 3)

  OutstandingRequestTracker tracker = new OutstandingRequestTracker()
  
  @Override
  String getTestName() {
    return "TestOutstandingRequestTracker"
  }

  @Test
  public void testAddRetrieveEntry() throws Throwable {
    OutstandingRequest request = tracker.newRequest(host1, 0)
    assert tracker.lookup(0, "host1").equals(request)
    assert tracker.remove(request).equals(request)
    assert !tracker.lookup(0, "host1")
  }

  @Test
  public void testAddCompleteEntry() throws Throwable {
    tracker.newRequest(host1, 0)
    tracker.newRequest(host2, 0)
    tracker.newRequest(host1, 1)
    assert tracker.onContainerAllocated(1, "host1")
    assert !tracker.lookup(1, "host1")
    assert tracker.lookup(0, "host1")
  }
  
  @Test
  public void testResetEntries() throws Throwable {
    OutstandingRequest r1 = tracker.newRequest(host1, 0)
    OutstandingRequest r2 = tracker.newRequest(host2, 0)
    OutstandingRequest r3 = tracker.newRequest(host1, 1)
    List<NodeInstance> canceled = tracker.resetOutstandingRequests(0)
    assert canceled.size() == 2
    assert canceled.contains(host1)
    assert canceled.contains(host2)
    assert tracker.lookup(1, "host1")
    assert !tracker.lookup(0, "host1")
    canceled = tracker.resetOutstandingRequests(0)
    assert canceled.size() == 0
    assert tracker.resetOutstandingRequests(1).size() == 1
  }

/*
  @Override
  AggregateConf buildInstanceDefinition() {
    def aggregateConf = super.buildInstanceDefinition()
    def component0 = aggregateConf.resourceOperations.getMandatoryComponent(ROLE0)
    component0.set(ResourceKeys.COMPONENT_PLACEMENT_POLICY, PlacementPolicy.STRICT)
    return aggregateConf
  }
*/

  @Test
  public void testEscalationOfStrictPlacement() throws Throwable {
    final def roleStatus = role1Status


    ProviderRole role = roleStatus.providerRole
    assert role.placementPolicy == PlacementPolicy.STRICT;
    Resource resource = new MockResource()

    appState.buildResourceRequirements(roleStatus, resource)

    // first requst
    OutstandingRequest r1 = tracker.newRequest(host1, roleStatus.key)
    final def initialRequest = r1.buildContainerRequest(resource, roleStatus, 0, null)
    assert r1.issuedRequest != null;
    assert r1.located
    assert !r1.escalated

    assert !initialRequest.relaxLocality
    assert tracker.listOutstandingRequests().size() == 1

    // simulate a few minutes; escalation MUST now be triggered
    List<AbstractRMOperation> escalations = tracker.escalateOutstandingRequests(180 * 1000)

    assert r1.escalated
    // two entries
    assert escalations.size() == 2;
    final def e1 = escalations[0]
    assert  e1 instanceof CancelSingleRequest
    CancelSingleRequest cancel = (CancelSingleRequest) e1
    assert initialRequest == cancel.request
    final def e2 = escalations[1]
    assert e2 instanceof ContainerRequestOperation;
    final def escRequest = (ContainerRequestOperation) e2
    def req2 = escRequest.request
    assert req2.relaxLocality

  }
}
