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

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.config.ClientConfig
import com.sun.jersey.api.json.JSONConfiguration
import com.sun.jersey.client.apache.ApacheHttpClient
import com.sun.jersey.client.apache.ApacheHttpClientHandler
import com.sun.jersey.client.apache.config.DefaultApacheHttpClientConfig
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager
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
import org.apache.slider.server.appmaster.rpc.RpcBinder
import org.junit.Test

import static org.apache.slider.server.appmaster.management.MetricsKeys.METRICS_LOGGING_ENABLED
import static org.apache.slider.server.appmaster.management.MetricsKeys.METRICS_LOGGING_LOG_INTERVAL
import static org.apache.slider.server.appmaster.web.rest.RestPaths.*

@CompileStatic
@Slf4j
class TestStandaloneREST extends AgentMiniClusterTestBase {


  @Test
  public void testStandaloneREST() throws Throwable {

    describe "create a standalone AM then perform actions on it"
    //launch fake master
    def conf = configuration
    conf.setBoolean(METRICS_LOGGING_ENABLED, true)
    conf.setInt(METRICS_LOGGING_LOG_INTERVAL, 1)
    String clustername = createMiniCluster("", conf, 1, true)


    ServiceLauncher<SliderClient> launcher =
        createStandaloneAMWithArgs(clustername,
            [Arguments.ARG_OPTION,
             AbstractRestTestDelegate.TEST_GLOBAL_OPTION, 
             AbstractRestTestDelegate.TEST_GLOBAL_OPTION_PRESENT],
            true, false)
    SliderClient client = launcher.service
    addToTeardown(client);

    ApplicationReport report = waitForClusterLive(client)
    def proxyAM = report.trackingUrl
    def directAM = report.originalTrackingUrl
    
    
    // set up url config to match
    initHttpTestSupport(launcher.configuration)


    execOperation(WEB_STARTUP_TIME) {
      GET(directAM)
    }
    
    execOperation(WEB_STARTUP_TIME) {
      def metrics = GET(directAM, SYSTEM_METRICS)
      log.info metrics
    }
    
    GET(proxyAM)

    log.info GET(proxyAM, SYSTEM_PING)
    log.info GET(proxyAM, SYSTEM_THREADS)
    log.info GET(proxyAM, SYSTEM_HEALTHCHECK)
    log.info GET(proxyAM, SYSTEM_METRICS_JSON)

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

    describe "Direct response headers from AM Web resources"
    def liveResUrl = appendToURL(directAM,
        SLIDER_PATH_APPLICATION, LIVE_RESOURCES);
    HttpOperationResponse response = executeGet(liveResUrl)
    response.headers.each { key, val -> log.info("$key $val") }
    log.info "Content type: ${response.contentType}"

    describe "proxied response headers from AM Web resources"
    response = executeGet(appendToURL(proxyAM,
        SLIDER_PATH_APPLICATION, LIVE_RESOURCES))
    response.headers.each { key, val -> log.info("$key $val") }
    log.info "Content type: ${response.contentType}"

    
    def ugiClient = createUGIJerseyClient();
    
    describe "Proxy SliderRestClient Tests"
    RestAPIClientTestDelegates proxySliderRestAPI =
        new RestAPIClientTestDelegates(proxyAM, ugiClient, proxyComplexVerbs)
    proxySliderRestAPI.testSuiteGetOperations()

    describe "Direct SliderRestClient Tests"
    RestAPIClientTestDelegates directSliderRestAPI =
        new RestAPIClientTestDelegates(directAM, ugiClient, directComplexVerbs)
    directSliderRestAPI.testSuiteAll()
    
    
    describe "Proxy Jersey Tests"
    JerseyTestDelegates proxyJerseyTests =
        new JerseyTestDelegates(proxyAM, ugiClient, proxyComplexVerbs)
    proxyJerseyTests.testSuiteGetOperations()

    describe "Direct Jersey Tests"

    JerseyTestDelegates directJerseyTests =
        new JerseyTestDelegates(directAM, ugiClient)
    directJerseyTests.testSuiteAll()

    describe "Direct Tests"

    LowLevelRestTestDelegates direct =
        new LowLevelRestTestDelegates(directAM, directComplexVerbs)
    direct.testSuiteAll()

    describe "Proxy Tests"

    LowLevelRestTestDelegates proxied = new LowLevelRestTestDelegates(proxyAM, proxyComplexVerbs)
    proxied.testSuiteAll()

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

    describe( "IPC equivalent operations")
    def sliderClusterProtocol = RpcBinder.getProxy(conf, report, 1000)
    SliderApplicationIpcClient ipcClient =
        new SliderApplicationIpcClient(sliderClusterProtocol)
    IpcApiClientTestDelegates ipcDelegates =
        new IpcApiClientTestDelegates(ipcClient)
    ipcDelegates.testSuiteAll()
    
    
    // log the metrics to show what's up
    direct.logCodahaleMetrics();

    // finally, stop the AM
    if (directComplexVerbs) {
      describe "Stopping AM via REST API"
      directSliderRestAPI.testStop();
    }
  }

  /**
   * Create Jersey client with URL handling by way
   * of the Apache HttpClient classes. 
   * @return a Jersey client
   */
  public static Client createJerseyClientHttpClient() {

    def httpclient = new HttpClient(new MultiThreadedHttpConnectionManager());
    httpclient.httpConnectionManager.params.connectionTimeout = 10000;
    ClientConfig clientConfig = new DefaultApacheHttpClientConfig();
    clientConfig.features[JSONConfiguration.FEATURE_POJO_MAPPING] = Boolean.TRUE;

    def handler = new ApacheHttpClientHandler(httpclient, clientConfig);

    def client = new ApacheHttpClient(handler)
    client.followRedirects = true
    return client;
  }
 
}
