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

import com.sun.jersey.api.client.Client
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.registry.client.api.RegistryOperations
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.webapp.ForbiddenException
import org.apache.slider.agent.rest.JerseyTestDelegates
import org.apache.slider.agent.rest.AbstractRestTestDelegate
import org.apache.slider.agent.rest.LowLevelRestTestDelegates
import org.apache.slider.agent.rest.RestAPIClientTestDelegates
import org.apache.slider.client.SliderClient
import org.apache.slider.client.rest.RestClientFactory
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
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

    /*
    Is the back door required? If so, don't test complex verbs via the proxy
    */
    def proxyComplexVerbs = !SliderXmlConfKeys.X_DEV_INSECURE_REQUIRED

    /*
     * Only do direct complex verbs if the no back door is needed, or if
     * it is enabled
     */
    def directComplexVerbs = proxyComplexVerbs || SLIDER_CONFIG.getBoolean(
        SliderXmlConfKeys.X_DEV_INSECURE_WS,
        SliderXmlConfKeys.X_DEV_INSECURE_DEFAULT)
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();
    SliderShell shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [ARG_OPTION,
         AbstractRestTestDelegate.TEST_GLOBAL_OPTION,
         AbstractRestTestDelegate.TEST_GLOBAL_OPTION_PRESENT],
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

    def proxyAM = report.url

    // get the root page, 
    getWebPage(proxyAM)
    
    def directAM = report.origTrackingUrl;
    // now attempt direct-to-AM pings
    LowLevelRestTestDelegates direct = new LowLevelRestTestDelegates(directAM,
        proxyComplexVerbs)

    direct.testSuiteAll()

    // and via the proxy
    LowLevelRestTestDelegates proxied = new LowLevelRestTestDelegates(proxyAM,
        directComplexVerbs)
    proxied.testSuiteAll()
    proxied.logCodahaleMetrics();

    describe "Proxy Jersey Tests"

    Client ugiClient = createUGIJerseyClient()
    JerseyTestDelegates proxyJerseyTests =
        new JerseyTestDelegates(proxyAM, ugiClient, proxyComplexVerbs)
    proxyJerseyTests.testSuiteGetOperations()

    describe "Direct Jersey Tests"
    JerseyTestDelegates directJerseyTests =
        new JerseyTestDelegates(directAM, ugiClient, directComplexVerbs)
    directJerseyTests.testSuiteAll()

    describe "Proxy SliderRestClient Tests"
    RestAPIClientTestDelegates proxySliderRestAPI =
        new RestAPIClientTestDelegates(proxyAM, ugiClient, proxyComplexVerbs)
    proxySliderRestAPI.testSuiteAll()

    describe "Direct SliderRestClient Tests"
    RestAPIClientTestDelegates directSliderRestAPI =
        new RestAPIClientTestDelegates(directAM, ugiClient, directComplexVerbs)
    directSliderRestAPI.testSuiteAll()

    if (UserGroupInformation.securityEnabled) {
      describe "Insecure Proxy Tests against a secure cluster"

      try {
        String rootpage = fetchWebPageRaisedErrorCodes(proxyAM);
        fail(" expected a 401, got $rootpage")
      } catch (ForbiddenException expected) {
        // expected
      }
      
      // these tests use the Jersey client without the Hadoop-specific
      // SPNEGO
      JerseyTestDelegates basicJerseyClientTests =
          new JerseyTestDelegates(proxyAM, createBasicJerseyClient())
      basicJerseyClientTests.testSuiteGetOperations()
    }

    // create the Rest client via the registry

    //get a slider client against the cluster

    SliderClient sliderClient = bondToCluster(SLIDER_CONFIG, CLUSTER)
    RegistryOperations operations = sliderClient.registryOperations;
    def restClientFactory = new RestClientFactory(
        operations, ugiClient,
        "~", SliderKeys.APP_TYPE, CLUSTER)
    def sliderApplicationApi = restClientFactory.createSliderApplicationApi();
    sliderApplicationApi.desiredModel
    sliderApplicationApi.resolvedModel
    if (proxyComplexVerbs) {
      sliderApplicationApi.ping("registry located")
    }
    
    // finally, stop the AM
    if (directComplexVerbs) {
      describe "Stopping AM via REST API"
      directSliderRestAPI.testStop();
    }
  }

}
