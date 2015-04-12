/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.agent.standalone

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.StatusKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class ApplicationWithAddonPackagesIT extends AgentCommandTestBase{
  
  static String CLUSTER = "test-application-with-add-on"

  static String APP_RESOURCE12 = "../slider-core/src/test/app_packages/test_min_pkg/nc_ping_cmd/resources.json"
  static String APP_META12 = "../slider-core/src/test/app_packages/test_min_pkg/nc_ping_cmd/metainfo.json"
  static String APP_TEMPLATE12 = "../slider-core/src/test/app_packages/test_min_pkg/nc_ping_cmd/appConfig.json"


  @Before
  public void prepareCluster() {
    setupCluster(CLUSTER)
  }

  @After
  public void destroyCluster() {
    cleanup(CLUSTER)
  }

  @Test
  public void testAgentRegistry() throws Throwable {
    describe("Create a cluster using metainfo, resources, and appConfig that calls nc to listen on a port")
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();

    SliderShell shell = createSliderApplicationMinPkg(CLUSTER,
        APP_META12,
        APP_RESOURCE12,
        APP_TEMPLATE12,
        [],
        launchReportFile)

    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)
  }
}