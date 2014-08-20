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

import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.Container
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.state.AppState
import org.apache.slider.server.appmaster.state.ContainerAssignment
import org.apache.slider.server.appmaster.state.RoleInstance
import org.junit.Test

@Slf4j
class TestMockAppStateFlexing extends BaseMockAppStateTest implements MockRoles {

  @Override
  String getTestName() {
    return "TestMockAppStateFlexing"
  }

  @Test
  public void testFlexDuringLaunchPhase() throws Throwable {
    role0Status.desired = 1

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    List<Container> allocations = engine.execute(ops)
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> releases = []
    appState.onContainersAllocated(allocations, assignments, releases)
    assert assignments.size() == 1
    ContainerAssignment assigned = assignments[0]
    Container target = assigned.container
    RoleInstance ri = roleInstance(assigned)

    ops = appState.reviewRequestAndReleaseNodes()
    assert ops.empty

    //now this is the start point.
    appState.containerStartSubmitted(target, ri);

    ops = appState.reviewRequestAndReleaseNodes()
    assert ops.empty

    RoleInstance ri2 = appState.innerOnNodeManagerContainerStarted(target.id)
  }

  @Test
  public void testFlexBeforeAllocationPhase() throws Throwable {
    role0Status.desired = 1

    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    assert !ops.empty
    List<AbstractRMOperation> ops2 = appState.reviewRequestAndReleaseNodes()
    assert ops2.empty
  }


  @Test
  public void testFlexDownTwice() throws Throwable {
    int r0 = 6
    int r1 = 0
    int r2 = 0
    role0Status.desired = r0
    role1Status.desired = r1
    role2Status.desired = r2
    List<RoleInstance> instances = createAndStartNodes()

    int clusterSize = r0 + r1 + r2
    assert instances.size() == clusterSize
    log.info("shrinking cluster")
    r0 = 4
    role0Status.desired = r0
    List<AppState.NodeCompletionResult> completionResults = []
    instances = createStartAndStopNodes(completionResults)
    assert instances.size() == 0
    // assert two nodes were released
    assert completionResults.size() == 2

    // no-op review
    completionResults = []
    instances = createStartAndStopNodes(completionResults)
    assert instances.size() == 0
    // assert two nodes were released
    assert completionResults.size() == 0
    
    
    // now shrink again
    role0Status.desired = r0 = 1
    completionResults = []
    instances = createStartAndStopNodes(completionResults)
    assert instances.size() == 0
    // assert two nodes were released
    assert completionResults.size() == 3

  }
  
  
}
