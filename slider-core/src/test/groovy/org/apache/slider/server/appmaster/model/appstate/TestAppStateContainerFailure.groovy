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
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.exceptions.TriggerClusterTeardownException
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine
import org.apache.slider.server.appmaster.state.*
import org.junit.Test

/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
@CompileStatic
@Slf4j
class TestAppStateContainerFailure extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  String getTestName() {
    return "TestAppStateContainerFailure"
  }

  /**
   * Small cluster with multiple containers per node,
   * to guarantee many container allocations on each node
   * @return
   */
  @Override
  MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(8000, 4)
  }

  @Test
  public void testShortLivedFail() throws Throwable {

    role0Status.desired = 1
    List<RoleInstance> instances = createAndStartNodes()
    assert instances.size() == 1

    RoleInstance instance = instances[0]
    long created = instance.createTime
    long started = instance.startTime
    assert created > 0
    assert started >= created
    List<ContainerId> ids = extractContainerIds(instances, 0)

    ContainerId cid = ids[0]
    assert appState.isShortLived(instance)
    AppState.NodeCompletionResult result = appState.onCompletedNode(containerStatus(cid, 1))
    assert result.roleInstance != null
    assert result.containerFailed 
    RoleStatus status = role0Status
    assert status.failed == 1
    assert status.startFailed == 1

    //view the world
    appState.getRoleHistory().dump();
    List<NodeInstance> queue = appState.roleHistory.cloneAvailableList(0)
    assert queue.size() == 0

  }

  @Test
  public void testLongLivedFail() throws Throwable {

    role0Status.desired = 1
    List<RoleInstance> instances = createAndStartNodes()
    assert instances.size() == 1

    RoleInstance instance = instances[0]
    instance.startTime = System.currentTimeMillis() - 60 * 60 * 1000;
    assert !appState.isShortLived(instance)
    List<ContainerId> ids = extractContainerIds(instances, 0)

    ContainerId cid = ids[0]
    AppState.NodeCompletionResult result = appState.onCompletedNode(
        containerStatus(cid, 1))
    assert result.roleInstance != null
    assert result.containerFailed
    RoleStatus status = role0Status
    assert status.failed == 1
    assert status.startFailed == 0

    //view the world
    appState.getRoleHistory().dump();
    List<NodeInstance> queue = appState.roleHistory.cloneAvailableList(0)
    assert queue.size() == 1

  }
  
  @Test
  public void testNodeStartFailure() throws Throwable {

    role0Status.desired = 1
    List<RoleInstance> instances = createAndSubmitNodes()
    assert instances.size() == 1

    RoleInstance instance = instances[0]

    List<ContainerId> ids = extractContainerIds(instances, 0)

    ContainerId cid = ids[0]
    appState.onNodeManagerContainerStartFailed(cid, new SliderException("oops"))
    RoleStatus status = role0Status
    assert status.failed == 1
    assert status.startFailed == 1

    
    RoleHistory history = appState.roleHistory
    history.dump();
    List<NodeInstance> queue = history.cloneAvailableList(0)
    assert queue.size() == 0

    NodeInstance ni = history.getOrCreateNodeInstance(instance.container)
    NodeEntry re = ni.get(0)
    assert re.failed == 1
    assert re.startFailed == 1
  }
  
  @Test
  public void testRecurrentStartupFailure() throws Throwable {

    role0Status.desired = 1
    try {
      for (int i = 0; i< 100; i++) {
        List<RoleInstance> instances = createAndSubmitNodes()
        assert instances.size() == 1

        List<ContainerId> ids = extractContainerIds(instances, 0)

        ContainerId cid = ids[0]
        log.info("$i instance $instances[0] $cid")
        assert cid 
        appState.onNodeManagerContainerStartFailed(cid, new SliderException("oops"))
        AppState.NodeCompletionResult result = appState.onCompletedNode(containerStatus(cid))
        assert result.containerFailed
      }
      fail("Cluster did not fail from too many startup failures")
    } catch (TriggerClusterTeardownException teardown) {
      log.info("Exception $teardown.exitCode : $teardown")
    }
  }

}
