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
import org.apache.hadoop.yarn.api.records.NodeState
import org.apache.slider.server.appmaster.model.mock.MockFactory
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.state.AppState
import org.junit.Test

/**
 * Test Anti-affine placement
 */
@CompileStatic
@Slf4j
class TestMockLabelledAAPlacement extends BaseMockAppStateAATest
    implements MockRoles {

  private int NODES = 3
  private int GPU_NODES = 2
  private String HOST0 = "00000000"
  private String HOST1 = "00000001"


  @Override
  void setup() {
    super.setup()
    // node 1 is GPU

    updateNodes(MockFactory.instance.newNodeReport(HOST0, NodeState.RUNNING, LABEL_GPU))
    updateNodes(MockFactory.instance.newNodeReport(HOST1, NodeState.RUNNING, LABEL_GPU))
  }

  @Override
  MockYarnEngine createYarnEngine() {
    new MockYarnEngine(NODES, 8)
  }

  void assertAllContainersAA() {
    assertAllContainersAA(gpuRole.key)
  }

  /**
   *
   * @throws Throwable
   */
  @Test
  public void testAskForTooMany() throws Throwable {

    describe("Ask for 1 more than the no of available nodes;" +
             " expect the final request to be unsatisfied until the cluster changes size")
    //more than expected
    int size = GPU_NODES
    gpuRole.desired = size + 1

    List<AbstractRMOperation > operations = appState.reviewRequestAndReleaseNodes()
    assert gpuRole.AARequestOutstanding

    assert gpuRole.pendingAntiAffineRequests == size
    for (int i = 0; i < size; i++) {
      def iter = "Iteration $i role = $aaRole"
      describe iter
      List<AbstractRMOperation > operationsOut = []

      def roleInstances = submitOperations(operations, [], operationsOut)
      // one instance per request
      assert 1 == roleInstances.size()
      appState.onNodeManagerContainerStarted(roleInstances[0].containerId)
      assertAllContainersAA()
      // there should be none left
      log.debug(nodeInformationSnapshotAsString())
      operations = operationsOut
      if (i + 1 < size) {
        assert operations.size() == 2
      } else {
        assert operations.size() == 1
      }
    }
    // expect an outstanding AA request to be unsatisfied
    assert gpuRole.actual < gpuRole.desired
    assert !gpuRole.requested
    assert !gpuRole.AARequestOutstanding
    List<Container> allocatedContainers = engine.execute(operations, [])
    assert 0 == allocatedContainers.size()
    // in a review now, no more requests can be generated, as there is no space for AA placements,
    // even though there is cluster capacity
    assert 0 == appState.reviewRequestAndReleaseNodes().size()

    // switch node 2 into being labelled
    def outcome = updateNodes(MockFactory.instance.
      newNodeReport("00000002", NodeState.RUNNING, "gpu"))

    assert cloneNodemap().size() == NODES
    assert outcome.clusterChanged
    // no active calls to empty
    assert outcome.operations.empty
    assert 1 == appState.reviewRequestAndReleaseNodes().size()
  }

  protected AppState.NodeUpdatedOutcome addNewNode() {
    updateNodes(MockFactory.instance.newNodeReport("00000004", NodeState.RUNNING, "gpu"))
  }

  @Test
  public void testClusterSizeChangesDuringRequestSequence() throws Throwable {
    describe("Change the cluster size where the cluster size changes during a test sequence.")
    gpuRole.desired = GPU_NODES + 1
    List<AbstractRMOperation> operations = appState.reviewRequestAndReleaseNodes()
    assert gpuRole.AARequestOutstanding
    assert GPU_NODES == gpuRole.pendingAntiAffineRequests
    def outcome = addNewNode()
    assert outcome.clusterChanged
    // one call to cancel
    assert 1 == outcome.operations.size()
    // and on a review, one more to rebuild
    assert 1 == appState.reviewRequestAndReleaseNodes().size()
  }

}
