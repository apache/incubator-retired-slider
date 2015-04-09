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
import org.apache.hadoop.fs.Path
import org.apache.slider.api.ResourceKeys
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.core.conf.ConfTreeOperations
import org.apache.slider.core.exceptions.BadConfigException
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockAppState
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine
import org.apache.slider.server.appmaster.state.MostRecentContainerReleaseSelector
import org.apache.slider.server.avro.LoadedRoleHistory
import org.apache.slider.server.avro.RoleHistoryWriter
import org.junit.Test

/**
 * Test that if you have >1 role, the right roles are chosen for release.
 */
@CompileStatic
@Slf4j
class TestMockAppStateFlexDynamicRoles extends BaseMockAppStateTest
    implements MockRoles {

  @Override
  String getTestName() {
    return "TestMockAppStateFlexDynamicRoles"
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
    appState = new MockAppState()
    appState.setContainerLimits(RM_MAX_RAM, RM_MAX_CORES)

    def instance = factory.newInstanceDefinition(0, 0, 0)

    def opts = [
        (ResourceKeys.COMPONENT_INSTANCES): "1",
        (ResourceKeys.COMPONENT_PRIORITY): "6",
    ]

    instance.resourceOperations.components["dynamic-6"] = opts

    
    appState.buildInstance(instance,
        new Configuration(),
        new Configuration(false),
        factory.ROLES,
        fs,
        historyPath,
        null, null,
        new MostRecentContainerReleaseSelector())
  }

  
  private ConfTreeOperations init() {
    createAndStartNodes();
    def resources = appState.instanceDefinition.resources;
    return new ConfTreeOperations(resources)
  }

  @Test
  public void testDynamicFlexAddRole() throws Throwable {
    def cd = init()
    def opts = [
        (ResourceKeys.COMPONENT_INSTANCES): "1",
        (ResourceKeys.COMPONENT_PRIORITY): "7",
    ]

    cd.components["dynamicAdd7"] = opts
    appState.updateResourceDefinitions(cd.confTree);
    createAndStartNodes();
    dumpClusterDescription("updated CD", appState.getClusterStatus())
    appState.lookupRoleStatus(7)
    appState.lookupRoleStatus(6)
    //gaps are still there
    try {
      assert null == appState.lookupRoleStatus(5)
    } catch (RuntimeException expected) {
    }
  }
  
  @Test
  public void testDynamicFlexAddRoleConflictingPriority() throws Throwable {
    def cd = init()
    def opts = [
        (ResourceKeys.COMPONENT_INSTANCES): "1",
        (ResourceKeys.COMPONENT_PRIORITY): "6",
    ]

    cd.components["conflictingPriority"] = opts
    try {
      appState.updateResourceDefinitions(cd.confTree);

      def status = appState.getClusterStatus()
      dumpClusterDescription("updated CD", status)
      fail("Expected an exception, got $status")
    } catch (BadConfigException expected) {
      log.info("Expected: {}", expected)
      log.debug("Expected: {}", expected, expected)
      // expected
    }
  }
  
  @Test
  public void testDynamicFlexDropRole() throws Throwable {
    def cd = init()
    cd.components.remove("dynamic");
    appState.updateResourceDefinitions(cd.confTree);

    def getCD = appState.getClusterStatus()
    dumpClusterDescription("updated CD", getCD)
    //status is retained for future
    appState.lookupRoleStatus(6)
  }


  @Test
  public void testHistorySaveFlexLoad() throws Throwable {
    def cd = init()
    def roleHistory = appState.roleHistory
    Path history = roleHistory.saveHistory(0x0001)
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    def opts = [
        (ResourceKeys.COMPONENT_INSTANCES): "1",
        (ResourceKeys.COMPONENT_PRIORITY): "9",
    ]

    cd.components["HistorySaveFlexLoad"] = opts
    appState.updateResourceDefinitions(cd.confTree);
    createAndStartNodes();
    def loadedRoleHistory = historyWriter.read(fs, history)
    assert 0 == appState.roleHistory.rebuild(loadedRoleHistory)
  }

  @Test
  public void testHistoryFlexSaveResetLoad() throws Throwable {
    def cd = init()
    def opts = [
        (ResourceKeys.COMPONENT_INSTANCES): "1",
        (ResourceKeys.COMPONENT_PRIORITY): "10",
    ]

    cd.components["HistoryFlexSaveLoad"] = opts
    appState.updateResourceDefinitions(cd.confTree);
    createAndStartNodes();
    RoleHistoryWriter historyWriter = new RoleHistoryWriter();
    def roleHistory = appState.roleHistory
    Path history = roleHistory.saveHistory(0x0002)
    //now reset the app state
    def historyWorkDir2 = new File("target/history" + testName + "-0002")
    def historyPath2 = new Path(historyWorkDir2.toURI())
    appState = new MockAppState()
    appState.setContainerLimits(RM_MAX_RAM, RM_MAX_CORES)
    appState.buildInstance(
        factory.newInstanceDefinition(0, 0, 0),
        new Configuration(),
        new Configuration(false),
        factory.ROLES,
        fs,
        historyPath2,
        null, null,
        new MostRecentContainerReleaseSelector())
    // on this read there won't be the right number of roles
    def loadedRoleHistory = historyWriter.read(fs, history)
    assert 0 == appState.roleHistory.rebuild(loadedRoleHistory)
  }

}
