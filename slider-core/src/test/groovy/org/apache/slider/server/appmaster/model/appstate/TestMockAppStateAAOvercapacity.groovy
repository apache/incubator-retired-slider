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

package org.apache.slider.server.appmaster.model.appstate

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.Container
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.state.AppState
import org.junit.Test

/**
 * Test Anti-affine placement with a cluster of size 1
 */
@CompileStatic
@Slf4j
class TestMockAppStateAAOvercapacity extends BaseMockAppStateAATest
    implements MockRoles {

  private int NODES = 1

  @Override
  MockYarnEngine createYarnEngine() {
    new MockYarnEngine(NODES, 1)
  }

  void assertAllContainersAA() {
    assertAllContainersAA(aaRole.key)
  }

  /**
   *
   * @throws Throwable
   */
  @Test
  public void testOvercapacityRecovery() throws Throwable {

    describe("Ask for 1 more than the no of available nodes;" +
             "verify the state. kill the allocated container and review")
    //more than expected
    long desired = 3
    aaRole.desired = desired
    assert appState.roleHistory.canPlaceAANodes()

    //first request
    List<AbstractRMOperation > operations = appState.reviewRequestAndReleaseNodes()
    assert aaRole.AARequestOutstanding
    assert desired - 1  == aaRole.pendingAntiAffineRequests
    List<AbstractRMOperation > operationsOut = []
    // allocate and re-submit
    def instances = submitOperations(operations, [], operationsOut)
    assert 1 == instances.size()
    assertAllContainersAA()

    // expect an outstanding AA request to be unsatisfied
    assert aaRole.actual < aaRole.desired
    assert !aaRole.requested
    assert !aaRole.AARequestOutstanding
    assert desired - 1 == aaRole.pendingAntiAffineRequests
    List<Container> allocatedContainers = engine.execute(operations, [])
    assert 0 == allocatedContainers.size()

    // now lets trigger a failure
    def nodemap = cloneNodemap()
    assert nodemap.size() == 1

    def instance = instances[0]
    def cid = instance.containerId

    AppState.NodeCompletionResult result = appState.onCompletedNode(containerStatus(cid,
        LauncherExitCodes.EXIT_TASK_LAUNCH_FAILURE))
    assert result.containerFailed

    assert aaRole.failed == 1
    assert aaRole.actual == 0
    def availablePlacements = appState.getRoleHistory().findNodeForNewAAInstance(aaRole)
    assert availablePlacements.size() == 1
    describe "expecting a successful review with available placements of $availablePlacements"
    operations = appState.reviewRequestAndReleaseNodes()
    assert operations.size() == 1
  }

 }
