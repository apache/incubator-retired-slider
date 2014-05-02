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
import org.apache.hadoop.conf.Configuration
import org.apache.slider.api.ResourceKeys
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockRecordFactory
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine
import org.apache.slider.server.appmaster.state.AbstractRMOperation
import org.apache.slider.server.appmaster.state.AppState
import org.apache.slider.server.appmaster.state.RoleInstance
import org.junit.Test

/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
@CompileStatic
@Slf4j
class TestAppStateDynamicRoles extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  String getTestName() {
    return "TestAppStateDynamicRoles"
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

  @Override
  void initApp() {
    super.initApp()
    appState = new AppState(new MockRecordFactory())
    appState.setContainerLimits(RM_MAX_RAM, RM_MAX_CORES)

    def instance = factory.newInstanceDefinition(0,0,0)

    def opts = [
        (ResourceKeys.COMPONENT_INSTANCES): "1",
        (ResourceKeys.COMPONENT_PRIORITY): "4",
    ]

    instance.resourceOperations.components["dynamic"]= opts
    
    
    appState.buildInstance(
        instance,
        new Configuration(false),
        factory.ROLES,
        fs,
        historyPath,
        null, null)
  }

  @Test
  public void testAllocateReleaseRealloc() throws Throwable {

    List<RoleInstance> instances = createAndStartNodes()
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    appState.getRoleHistory().dump();
    
  }
  
}
