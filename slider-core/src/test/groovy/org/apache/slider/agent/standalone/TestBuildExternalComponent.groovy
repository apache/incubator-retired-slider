/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.agent.standalone

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.api.ResourceKeys
import org.apache.slider.api.RoleKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.SliderActions
import org.apache.slider.common.tools.SliderFileSystem
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.providers.agent.AgentKeys
import org.junit.Test

import static org.apache.slider.common.params.Arguments.*

@CompileStatic
@Slf4j

class TestBuildExternalComponent extends AgentMiniClusterTestBase {

  @Test
  public void testExternalComponentBuild() throws Throwable {
    String clustername = createMiniCluster("", configuration, 1, true)

    describe "verify external components"

    String echo = "echo"
    ServiceLauncher<SliderClient> launcher = createOrBuildCluster(
        SliderActions.ACTION_BUILD,
        clustername,
        [(echo): 1],
        [ARG_RES_COMP_OPT, echo, ResourceKeys.COMPONENT_PRIORITY, "2"],
        true,
        false,
        agentDefOptions)
    SliderClient sliderClient = launcher.service
    addToTeardown(sliderClient);

    // verify the cluster exists
    assert 0 == sliderClient.actionExists(clustername, false)

    String parent1 = clustername + "_ext"
    launcher = createOrBuildCluster(
      SliderActions.ACTION_BUILD,
      parent1,
      [(echo): 1],
      [ARG_COMP_OPT, clustername, COMPONENT_TYPE_KEY, COMPONENT_TYPE_EXTERNAL_APP,
       ARG_RES_COMP_OPT, echo, ResourceKeys.COMPONENT_PRIORITY, "3"],
      true,
      false,
      agentDefOptions)
    sliderClient = launcher.service
    addToTeardown(sliderClient);

    // verify the cluster exists
    assert 0 == sliderClient.actionExists(parent1, false)
    // verify generated conf
    def aggregateConf = sliderClient.loadPersistedClusterDescription(parent1)
    assert 3 == aggregateConf.resourceOperations.componentNames.size()
    assert aggregateConf.resourceOperations.componentNames.contains(COMPONENT_AM)
    assert aggregateConf.resourceOperations.componentNames.contains(echo)
    assert aggregateConf.resourceOperations.componentNames.contains(clustername + COMPONENT_SEPARATOR + echo)

    aggregateConf.resolve()
    assert aggregateConf.appConfOperations.get(AgentKeys.APP_DEF).equals(
      aggregateConf.appConfOperations.getComponentOpt(echo, AgentKeys.APP_DEF,
        aggregateConf.appConfOperations.get(AgentKeys.APP_DEF)))
    SliderFileSystem sfs = createSliderFileSystem()
    String appdefdir = sfs.buildAppDefDirPath(parent1)
    checkComponent(aggregateConf, clustername + COMPONENT_SEPARATOR + echo, appdefdir)

    String parent2 = "parent"
    launcher = createOrBuildCluster(
      SliderActions.ACTION_BUILD,
      parent2,
      [(echo): 1],
      [ARG_COMP_OPT, clustername, COMPONENT_TYPE_KEY, COMPONENT_TYPE_EXTERNAL_APP,
       ARG_COMP_OPT, parent1, COMPONENT_TYPE_KEY, COMPONENT_TYPE_EXTERNAL_APP,
       ARG_RES_COMP_OPT, echo, ResourceKeys.COMPONENT_PRIORITY, "4"],
      true,
      false,
      agentDefOptions)
    sliderClient = launcher.service
    addToTeardown(sliderClient);

    // verify the cluster exists
    assert 0 == sliderClient.actionExists(parent2, false)
    // verify generated conf
    aggregateConf = sliderClient.loadPersistedClusterDescription(parent2)
    assert 5 == aggregateConf.resourceOperations.componentNames.size()
    assert aggregateConf.resourceOperations.componentNames.contains(COMPONENT_AM)
    assert aggregateConf.resourceOperations.componentNames.contains(echo)
    assert aggregateConf.resourceOperations.componentNames.contains(clustername + COMPONENT_SEPARATOR + echo)
    assert aggregateConf.resourceOperations.componentNames.contains(parent1 + COMPONENT_SEPARATOR + echo)
    assert aggregateConf.resourceOperations.componentNames.contains(parent1 + COMPONENT_SEPARATOR + clustername + COMPONENT_SEPARATOR + echo)

    aggregateConf.resolve()
    assert aggregateConf.appConfOperations.get(AgentKeys.APP_DEF).equals(
      aggregateConf.appConfOperations.getComponentOpt(echo, AgentKeys.APP_DEF,
        aggregateConf.appConfOperations.get(AgentKeys.APP_DEF)))
    appdefdir = sfs.buildAppDefDirPath(parent2)
    checkComponent(aggregateConf, clustername + COMPONENT_SEPARATOR + echo, appdefdir)
    checkComponent(aggregateConf, parent1 + COMPONENT_SEPARATOR + echo, appdefdir)
    checkComponent(aggregateConf, parent1 + COMPONENT_SEPARATOR + clustername + COMPONENT_SEPARATOR + echo, appdefdir)
  }

  private void checkComponent(AggregateConf aggConf, String component,
                              String appdefdir) {
    String path = new Path(appdefdir, SliderUtils.trimPrefix(
      aggConf.appConfOperations.getComponentOpt(component, RoleKeys
        .ROLE_PREFIX, null)) + "_" + SliderKeys.DEFAULT_APP_PKG).toString()
    assert path.equals(aggConf.appConfOperations.getComponentOpt(component,
      AgentKeys.APP_DEF, null))
  }
}
