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
import org.apache.slider.server.appmaster.model.mock.MockPriority
import org.apache.slider.server.appmaster.model.mock.MockResource
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.operations.CancelSingleRequest
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation
import org.apache.slider.server.appmaster.state.ContainerAllocationOutcome
import org.apache.slider.server.appmaster.state.ContainerPriority
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
    assert tracker.lookupPlacedRequest(0, "host1").equals(request)
    assert tracker.removePlacedRequest(request).equals(request)
    assert !tracker.lookupPlacedRequest(0, "host1")
  }

  @Test
  public void testAddCompleteEntry() throws Throwable {
    tracker.newRequest(host1, 0)
    tracker.newRequest(host2, 0)
    tracker.newRequest(host1, 1)
    assert tracker.onContainerAllocated(1, "host1", null).outcome == ContainerAllocationOutcome.Placed
    assert !tracker.lookupPlacedRequest(1, "host1")
    assert tracker.lookupPlacedRequest(0, "host1")
  }

  @Test
  public void testResetOpenRequests() throws Throwable {
    def req1 = tracker.newRequest(null, 0)
    assert !req1.located
    tracker.newRequest(host1, 0)
    def openRequests = tracker.listOpenRequests()
    assert openRequests.size() == 1
    tracker.resetOutstandingRequests(0)
    assert tracker.listOpenRequests().empty
    assert tracker.listPlacedRequests().empty
  }

  @Test
  public void testRemoveOpenRequestUnissued() throws Throwable {
    def req1 = tracker.newRequest(null, 0)
    assert tracker.listOpenRequests().size() == 1
    def c1 = factory.newContainer()
    c1.setPriority(new MockPriority(0))

    def resource = factory.newResource()
    resource.virtualCores=1
    resource.memory = 48;
    c1.setResource(resource)
    ContainerAllocationOutcome outcome = tracker.onContainerAllocated(0, "host1", c1).outcome
    assert outcome == ContainerAllocationOutcome.Unallocated
    assert tracker.listOpenRequests().size() == 1
  }

  @Test
  public void testIssuedOpenRequest() throws Throwable {
    def req1 = tracker.newRequest(null, 0)
    def resource = factory.newResource()
    resource.virtualCores = 1
    resource.memory = 48;
    def yarnRequest = req1.buildContainerRequest(resource, role0Status, 0, "")
    assert tracker.listOpenRequests().size() == 1
    def c1 = factory.newContainer()

    def nodeId = factory.newNodeId()
    c1.nodeId = nodeId
    nodeId.host ="hostname-1"

    def pri = ContainerPriority.buildPriority(0, false)
    assert pri > 0
    c1.setPriority(new MockPriority(pri))

    c1.setResource(resource)

    def issued = req1.issuedRequest
    assert issued.capability == resource
    assert issued.priority.priority == c1.getPriority().getPriority()
    assert req1.resourceRequirementsMatch(resource)

    def allocation = tracker.onContainerAllocated(0, nodeId.host, c1)
    assert tracker.listOpenRequests().size() == 0
    assert allocation.outcome == ContainerAllocationOutcome.Open
    assert allocation.origin.is(req1)
  }

  @Test
  public void testResetEntries() throws Throwable {
    tracker.newRequest(host1, 0)
    tracker.newRequest(host2, 0)
    tracker.newRequest(host1, 1)
    List<NodeInstance> canceled = tracker.resetOutstandingRequests(0)
    assert canceled.size() == 2
    assert canceled.contains(host1)
    assert canceled.contains(host2)
    assert tracker.lookupPlacedRequest(1, "host1")
    assert !tracker.lookupPlacedRequest(0, "host1")
    canceled = tracker.resetOutstandingRequests(0)
    assert canceled.size() == 0
    assert tracker.resetOutstandingRequests(1).size() == 1
  }

  @Test
  public void testEscalation() throws Throwable {

    // first request: default placement
    assert role0Status.placementPolicy == PlacementPolicy.DEFAULT;
    final def (res0, outstanding0) = newRequest(role0Status)
    final def initialRequest = outstanding0.buildContainerRequest(res0, role0Status, 0, null)
    assert outstanding0.issuedRequest != null;
    assert outstanding0.located
    assert !outstanding0.escalated
    assert !initialRequest.relaxLocality
    assert tracker.listPlacedRequests().size() == 1

    // second. This one doesn't get launched. This is to verify that the escalation
    // process skips entries which are in the list but have not been issued.
    // ...which can be a race condition between request issuance & escalation.
    // (not one observed outside test authoring, but retained for completeness)
    assert role2Status.placementPolicy == PlacementPolicy.ANTI_AFFINITY_REQUIRED
    def (res2, outstanding2) = newRequest(role2Status)

    // simulate some time escalation of role 1 MUST now be triggered
    final def interval = role0Status.placementTimeoutSeconds * 1000 + 500
    def now = interval
    final List<AbstractRMOperation> escalations = tracker.escalateOutstandingRequests(now)

    assert outstanding0.escalated
    assert !outstanding2.escalated

    // two entries
    assert escalations.size() == 2;
    final def e1 = escalations[0]
    assert  e1 instanceof CancelSingleRequest
    final CancelSingleRequest cancel = (CancelSingleRequest) e1
    assert initialRequest == cancel.request
    final def e2 = escalations[1]
    assert e2 instanceof ContainerRequestOperation;
    final def escRequest = (ContainerRequestOperation) e2
    assert escRequest.request.relaxLocality

    // build that second request from an anti-affine entry
    // these get placed as well
    now += interval
    final def containerReq2 = outstanding2.buildContainerRequest(res2, role2Status, now, null)
    // escalate a little bit more
    final List<AbstractRMOperation> escalations2 = tracker.escalateOutstandingRequests(now)
    // and expect no new entries
    assert escalations2.size() == 0

    // go past the role2 timeout
    now += role2Status.placementTimeoutSeconds * 1000 + 500
    // escalate a little bit more
    final List<AbstractRMOperation> escalations3 = tracker.escalateOutstandingRequests(now)
    // and expect another escalation
    assert escalations3.size() == 2
    assert outstanding2.escalated

    // finally add a strict entry to the mix
    def (res3, outstanding3) = newRequest(role1Status)
    final ProviderRole providerRole1 = role1Status.providerRole
    assert providerRole1.placementPolicy == PlacementPolicy.STRICT
    now += interval
    assert !outstanding3.mayEscalate
    final List<AbstractRMOperation> escalations4 = tracker.escalateOutstandingRequests(now)
    assert escalations4.empty

  }

  /**
   * If the placement does include a label, the initial request must
   * <i>not</i> include it.
   * The escalation request will contain the label, while
   * leaving out the node list.
   * retains the node list, but sets relaxLocality==true
   * @throws Throwable
   */
  @Test
  public void testRequestLabelledPlacement() throws Throwable {
    NodeInstance ni = new NodeInstance("host1", 0)
    def req1 = tracker.newRequest(ni, 0)
    def resource = factory.newResource()
    resource.virtualCores = 1
    resource.memory = 48;

    def label = "workers"
    // initial request
    def yarnRequest = req1.buildContainerRequest(resource, role0Status, 0, label)
    assert (yarnRequest.nodeLabelExpression == null)
    assert (!yarnRequest.relaxLocality)
    def yarnRequest2 = req1.escalate()
    assert (yarnRequest2.nodeLabelExpression == label)
    assert (yarnRequest2.relaxLocality)
  }

  /**
   * If the placement doesnt include a lablel, then the escalation request
   * retains the node list, but sets relaxLocality==true
   * @throws Throwable
   */
  @Test
  public void testRequestUnlabelledPlacement() throws Throwable {
    NodeInstance ni = new NodeInstance("host1", 0)
    def req1 = tracker.newRequest(ni, 0)
    def resource = factory.newResource()
    resource.virtualCores = 1
    resource.memory = 48;

    def label = null
    // initial request
    def yarnRequest = req1.buildContainerRequest(resource, role0Status, 0, label)
    assert (yarnRequest.nodes != null)
    assert (yarnRequest.nodeLabelExpression == null)
    assert (!yarnRequest.relaxLocality)
    def yarnRequest2 = req1.escalate()
    assert (yarnRequest2.nodes != null)
    assert (yarnRequest2.relaxLocality)
  }


  /**
   * Create a new request (always against host1)
   * @param r role status
   * @return (resource, oustanding-request)
   */
  public def newRequest(RoleStatus r) {
    final Resource res2 = new MockResource()
    appState.buildResourceRequirements(r, res2)
    final OutstandingRequest outstanding2 = tracker.newRequest(host1, r.key)
    return [res2, outstanding2]
  }
}
