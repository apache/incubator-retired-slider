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
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.state.RoleInstance
import org.junit.Test

/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
@CompileStatic
@Slf4j
class TestMockAppStateRoleRelease extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  String getTestName() {
    return "TestMockAppStateRoleRelease"
  }

  /**
   * Small cluster with multiple containers per node,
   * to guarantee many container allocations on each node
   * @return
   */
  @Override
  MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(4, 4)
  }

  @Test
  public void testAllocateReleaseRealloc() throws Throwable {
    /**
     * Allocate to all nodes
     */
    role0Status.desired = 6
    role1Status.desired = 5
    role2Status.desired = 4
    List<RoleInstance> instances = createAndStartNodes()
    assert instances.size() == 15

    //now it is surplus
    role0Status.desired = 0
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    
    List<ContainerId> released = []
    engine.execute(ops, released)
    List<ContainerId> ids = extractContainerIds(instances, 0)
    released.each { ContainerId cid ->
      assert appState.onCompletedNode(containerStatus(cid)).roleInstance
      assert ids.contains(cid)
    }

    //view the world
    appState.getRoleHistory().dump();
    
  }

}
