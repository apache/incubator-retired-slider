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

package org.apache.slider.funtest.lifecycle

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.agent.rest.RestTestDelegates
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.common.tools.ConfigHelper
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class AgentWebPagesIT extends AgentCommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {


  static String CLUSTER = "test-agent-web"

  static String APP_RESOURCE2 = "../slider-core/src/test/app_packages/test_command_log/resources_no_role.json"

  @Before
  public void prepareCluster() {
    setupCluster(CLUSTER)
  }

  @After
  public void destroyCluster() {
    cleanup(CLUSTER)
  }

  @Test
  public void testAgentWeb() throws Throwable {
    describe("Web queries & REST operations against an AM")
    
    // verify the ws/ path is open for all HTTP verbs
    def sliderConfiguration = ConfigHelper.loadSliderConfiguration();

    def wsBackDoorRequired = SLIDER_CONFIG.getBoolean(
        SliderXmlConfKeys.X_DEV_INSECURE_WS,
        true)
    assert wsBackDoorRequired ==
        sliderConfiguration.getBoolean(
            SliderXmlConfKeys.X_DEV_INSECURE_WS,
            false)
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();
    SliderShell shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [Arguments.ARG_OPTION,
         RestTestDelegates.TEST_GLOBAL_OPTION,
         RestTestDelegates.TEST_GLOBAL_OPTION_PRESENT],
        launchReportFile)

    logShell(shell)

    def conf = SLIDER_CONFIG

    initHttpTestSupport(conf)

    def appId = ensureYarnApplicationIsUp(launchReportFile)
    assert appId
    expectRootWebPageUp(appId, instanceLaunchTime)
    File liveReportFile = createTempJsonFile();

    lookup(appId,liveReportFile)
    def report = loadAppReport(liveReportFile)
    assert report.url

    def appmaster = report.url

    // get the root page, 
    getWebPage(appmaster)
    
    def realappmaster = report.origTrackingUrl;
    // now attempt direct-to-AM pings
    RestTestDelegates proxied = new RestTestDelegates(appmaster)
    RestTestDelegates direct = new RestTestDelegates(realappmaster)

    proxied.testCodahaleOperations()
    direct.testCodahaleOperations()
    proxied.testLiveResources()

    proxied.testRESTModel()

    direct.testRestletGetOperations()
    proxied.testRestletGetOperations()

    // PUT & POST &c direct
    direct.testPing()
    if (!wsBackDoorRequired) {
      // and via the proxy
      proxied.testRESTModel()
    }
    
    direct.logCodahaleMetrics();

    // finally, stop the AM
    direct.testStop();
  }

}
