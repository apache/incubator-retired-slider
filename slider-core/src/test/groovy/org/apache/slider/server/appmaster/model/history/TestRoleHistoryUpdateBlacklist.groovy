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

import org.apache.slider.server.appmaster.actions.ResetFailureWindow
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockAM
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.model.mock.MockRMOperationHandler
import org.apache.slider.server.appmaster.model.mock.MockRoleHistory
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.operations.UpdateBlacklistOperation
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.RoleHistory
import org.apache.slider.server.appmaster.state.RoleStatus
import org.junit.Before
import org.junit.Test

class TestRoleHistoryUpdateBlacklist extends BaseMockAppStateTest {
  RoleHistory roleHistory = new MockRoleHistory(MockFactory.ROLES)
  Collection<RoleStatus> roleStatuses = [new RoleStatus(MockFactory.PROVIDER_ROLE0)]
  NodeInstance ni = nodeInstance(1, 0, 0, 0)
  List<NodeInstance> nodes = [ni]

  @Override
  String getTestName() {
    return "TestUpdateBlacklist"
  }

  @Before
  public void setupNodeMap() {
    roleHistory.insert(nodes)
    roleHistory.buildRecentNodeLists();
    appState.roleHistory = roleHistory
  }

  @Test
  public void testUpdateBlacklist() {
    assert !ni.isBlacklisted()

    // at threshold, blacklist is unmodified
    recordAsFailed(ni, 0, MockFactory.NODE_FAILURE_THRESHOLD)
    def op = roleHistory.updateBlacklist(roleStatuses)
    assert null == op
    assert !ni.isBlacklisted()

    // threshold is reached, node goes on blacklist
    recordAsFailed(ni, 0, 1)
    op = roleHistory.updateBlacklist(roleStatuses)
    assert null != op
    assert ni.isBlacklisted()

    // blacklist remains unmodified
    op = roleHistory.updateBlacklist(roleStatuses)
    assert null == op
    assert ni.isBlacklisted()

    // failure threshold reset, node goes off blacklist
    ni.resetFailedRecently()
    op = roleHistory.updateBlacklist(roleStatuses)
    assert null != op
    assert !ni.isBlacklisted()
  }

  @Test
  public void testBlacklistOperations() {
    recordAsFailed(ni, 0, MockFactory.NODE_FAILURE_THRESHOLD + 1)

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assertListLength(ops, 1)
    AbstractRMOperation op = ops[0]
    assert op instanceof UpdateBlacklistOperation
    assert ni.isBlacklisted()

    MockRMOperationHandler handler = new MockRMOperationHandler()
    assert 0 == handler.blacklisted
    handler.execute(ops)
    assert 1 == handler.blacklisted

    ResetFailureWindow resetter = new ResetFailureWindow(handler);
    resetter.execute(new MockAM(), null, appState)
    assert 0 == handler.blacklisted
    assert !ni.isBlacklisted()
  }
}
