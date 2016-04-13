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
import org.apache.hadoop.fs.Path
import org.apache.slider.api.ResourceKeys
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.conf.ConfTreeOperations
import org.apache.slider.core.exceptions.BadConfigException
import org.apache.slider.server.appmaster.model.mock.BaseMockAppStateTest
import org.apache.slider.server.appmaster.model.mock.MockAppState
import org.apache.slider.server.appmaster.model.mock.MockRoles
import org.apache.slider.server.appmaster.model.mock.MockYarnEngine
import org.apache.slider.server.appmaster.state.AppStateBindingInfo
import org.apache.slider.server.appmaster.state.MostRecentContainerReleaseSelector
import org.apache.slider.server.avro.RoleHistoryWriter
import org.junit.Test

/**
 * Test that if you have more than one role, the right roles are chosen for release.
 */
@CompileStatic
@Slf4j
class TestMockAppStateUniqueNames extends BaseMockAppStateTest
  implements MockRoles {

  @Override
  String getTestName() {
    return "TestMockAppStateUniqueNames"
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
  AppStateBindingInfo buildBindingInfo() {
    def bindingInfo = super.buildBindingInfo()
    bindingInfo.releaseSelector = new MostRecentContainerReleaseSelector()
    bindingInfo
  }

  @Override
  AggregateConf buildInstanceDefinition() {
    def instance = factory.newInstanceDefinition(0, 0, 0)

    def opts = [
      (ResourceKeys.COMPONENT_INSTANCES): "1",
      (ResourceKeys.COMPONENT_PRIORITY) : "6",
      (ResourceKeys.UNIQUE_NAMES) : "true",
    ]

    instance.resourceOperations.components["group1"] = opts
    instance
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
      (ResourceKeys.COMPONENT_INSTANCES): "2",
      (ResourceKeys.COMPONENT_PRIORITY): "7",
      (ResourceKeys.UNIQUE_NAMES) : "true",
    ]

    cd.components["group2"] = opts
    appState.updateResourceDefinitions(cd.confTree);
    createAndStartNodes();
    dumpClusterDescription("updated CD", appState.getClusterStatus())
    assert 1 == appState.lookupRoleStatus("group11").desired
    assert 1 == appState.lookupRoleStatus("group21").desired
    assert 1 == appState.lookupRoleStatus("group22").desired
    assert 6 == appState.lookupRoleStatus("group11").priority
    assert 7 == appState.lookupRoleStatus("group21").priority
    assert 8 == appState.lookupRoleStatus("group22").priority
  }

  @Test
  public void testDynamicFlexDown() throws Throwable {
    def cd = init()
    def opts = [
      (ResourceKeys.COMPONENT_INSTANCES): "0",
      (ResourceKeys.COMPONENT_PRIORITY) : "6",
      (ResourceKeys.UNIQUE_NAMES) : "true",
    ]

    cd.components["group1"] = opts
    appState.updateResourceDefinitions(cd.confTree);
    createAndStartNodes();
    dumpClusterDescription("updated CD", appState.getClusterStatus())
    appState.lookupRoleStatus(6)
    assert 0 == appState.lookupRoleStatus("group11").desired
    assert 6 == appState.lookupRoleStatus("group11").priority
  }

  @Test
  public void testDynamicFlexUp() throws Throwable {
    def cd = init()
    def opts = [
      (ResourceKeys.COMPONENT_INSTANCES): "3",
      (ResourceKeys.COMPONENT_PRIORITY) : "6",
      (ResourceKeys.UNIQUE_NAMES) : "true",
    ]

    cd.components["group1"] = opts
    appState.updateResourceDefinitions(cd.confTree);
    createAndStartNodes();
    dumpClusterDescription("updated CD", appState.getClusterStatus())
    appState.lookupRoleStatus(6)
    appState.lookupRoleStatus(7)
    appState.lookupRoleStatus(8)
    assert 1 == appState.lookupRoleStatus("group11").desired
    assert 1 == appState.lookupRoleStatus("group12").desired
    assert 1 == appState.lookupRoleStatus("group13").desired
    assert 6 == appState.lookupRoleStatus("group11").priority
    assert 7 == appState.lookupRoleStatus("group12").priority
    assert 8 == appState.lookupRoleStatus("group13").priority
  }

}
