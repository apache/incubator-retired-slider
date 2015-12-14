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

package org.apache.slider.agent.rest

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.registry.client.api.RegistryOperations
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.client.ipc.SliderApplicationIpcClient
import org.apache.slider.client.rest.RestClientFactory
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.restclient.HttpOperationResponse
import static org.apache.slider.server.appmaster.management.MetricsKeys.METRICS_LOGGING_ENABLED
import static org.apache.slider.server.appmaster.management.MetricsKeys.METRICS_LOGGING_LOG_INTERVAL
import org.apache.slider.server.appmaster.rpc.RpcBinder
import static org.apache.slider.server.appmaster.web.rest.RestPaths.LIVE_RESOURCES
import static org.apache.slider.server.appmaster.web.rest.RestPaths.SLIDER_PATH_APPLICATION
import static org.apache.slider.server.appmaster.web.rest.RestPaths.SYSTEM_HEALTHCHECK
import static org.apache.slider.server.appmaster.web.rest.RestPaths.SYSTEM_METRICS_JSON
import static org.apache.slider.server.appmaster.web.rest.RestPaths.SYSTEM_PING
import static org.apache.slider.server.appmaster.web.rest.RestPaths.SYSTEM_THREADS
import org.junit.Test

@CompileStatic
@Slf4j
class TestHttpsAM extends AgentMiniClusterTestBase  {

  @Test
  public void testHttpsAM() throws Throwable {

    describe "create a standalone AM then perform actions on it"
    //launch fake master
    def conf = configuration
    conf.setBoolean(METRICS_LOGGING_ENABLED, true)
    conf.setInt(METRICS_LOGGING_LOG_INTERVAL, 1)
    // HTTPS
    conf.setBoolean(KEY_SLIDER_AM_HTTPS, true)
    // and AM opens up WS without redirection
    conf.setBoolean(X_DEV_INSECURE_WS, true)
    String clustername = createMiniCluster("", conf, 1, true)

    ServiceLauncher<SliderClient> launcher =
        createStandaloneAMWithArgs(clustername,
            [
              Arguments.ARG_OPTION,
              AbstractRestTestDelegate.TEST_GLOBAL_OPTION,
              AbstractRestTestDelegate.TEST_GLOBAL_OPTION_PRESENT,
              Arguments.ARG_COMP_OPT, COMPONENT_AM, KEY_SLIDER_AM_HTTPS, "true"
            ],
            true,
            false)
    SliderClient client = launcher.service
    addToTeardown(client);

    ApplicationReport report = waitForClusterLive(client)
    def directAM = report.originalTrackingUrl
    assert directAM.startsWith("https:")

    // set up url config to match
    initHttpTestSupport(launcher.configuration)

    execOperation(WEB_STARTUP_TIME) {
      GET(directAM)
    }

    execOperation(WEB_STARTUP_TIME) {
      def metrics = GET(directAM, SYSTEM_METRICS_JSON)
      log.info prettyPrintJson(metrics)
    }

    //  Only do direct complex verbs if the no back door is needed, or if
    //  it is enabled
    def directComplexVerbs = false

    def ugiClient = createUGIJerseyClient();

    describe "Direct SliderRestClient Tests"
    RestAPIClientTestDelegates directSliderRestAPI =
        new RestAPIClientTestDelegates(directAM, ugiClient, directComplexVerbs)
    directSliderRestAPI.testSuiteAll()

    describe "Direct Jersey Tests"

    JerseyTestDelegates directJerseyTests = new JerseyTestDelegates(directAM, ugiClient)
    directJerseyTests.testSuiteAll()

    describe "Direct Tests"

    LowLevelRestTestDelegates direct = new LowLevelRestTestDelegates(directAM, directComplexVerbs)
    direct.testSuiteAll()

    // create the Rest client via the registry

    RegistryOperations operations = client.registryOperations;
    def restClientFactory = new RestClientFactory(
        operations,
        ugiClient,
        "~", SliderKeys.APP_TYPE,
        clustername)
    def sliderApplicationApi = restClientFactory.createSliderAppApiClient();
    sliderApplicationApi.desiredModel
    sliderApplicationApi.resolvedModel

    if (directComplexVerbs) {
      sliderApplicationApi.ping("registry located")
    }

    // log the metrics to show what's up
    direct.logCodahaleMetrics();

    // finally, stop the AM
    if (directComplexVerbs) {
      describe "Stopping AM via REST API"
      directSliderRestAPI.testStop();
    }
  }

}
