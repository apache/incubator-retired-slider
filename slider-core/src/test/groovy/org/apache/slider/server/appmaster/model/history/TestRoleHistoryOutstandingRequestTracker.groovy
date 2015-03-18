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
import org.apache.slider.server.appmaster.state.RoleStatus
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
  public void testEscalation() throws Throwable {
    ProviderRole providerRole1 = role1Status.providerRole
    assert providerRole1.placementPolicy == PlacementPolicy.STRICT;
    // first request
    final def (res1, outstanding1) = newRequest(role1Status)
    final def initialRequest = outstanding1.buildContainerRequest(res1, role1Status, 0, null)
    assert outstanding1.issuedRequest != null;
    assert outstanding1.located
    assert !outstanding1.escalated
    assert !initialRequest.relaxLocality
    assert tracker.listOutstandingRequests().size() == 1

    // second. This one doesn't get launched. This is to verify that the escalation
    // process skips entries which are in the list but have not been issued.
    // ...which can be a race condition between request issuance & escalation.
    // (not one observed outside test authoring, but retained for completeness)
    assert role2Status.placementPolicy == PlacementPolicy.ANTI_AFFINITY_REQUIRED
    def (res2, outstanding2) = newRequest(role2Status)

    // simulate some time escalation of role 1 MUST now be triggered
    List<AbstractRMOperation> escalations =
        tracker.escalateOutstandingRequests(providerRole1.placementTimeoutSeconds * 1000 + 500 )

    assert outstanding1.escalated
    assert !outstanding2.escalated

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

    def (res3, outstanding3) = newRequest(role2Status)
    outstanding3.buildContainerRequest(res3, role2Status, 0, null)

    List<AbstractRMOperation> escalations2 =
        tracker.escalateOutstandingRequests(providerRole1.placementTimeoutSeconds * 1000 + 500)
    assert escalations2.size() == 0
  }

  public def newRequest(RoleStatus r) {
    final Resource res2 = new MockResource()
    appState.buildResourceRequirements(r, res2)
    final OutstandingRequest outstanding2 = tracker.newRequest(host1, r.key)
    return [res2, outstanding2]
  }
}
