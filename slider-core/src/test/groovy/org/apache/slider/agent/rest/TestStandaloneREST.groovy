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
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.common.params.Arguments
import org.apache.slider.server.appmaster.web.rest.application.ApplicationResource
import org.apache.slider.client.SliderClient
import org.apache.slider.core.main.ServiceLauncher

import static org.apache.slider.server.appmaster.web.rest.RestPaths.*;
import org.junit.Test

import static org.apache.slider.server.appmaster.management.MetricsKeys.*

@CompileStatic
@Slf4j
class TestStandaloneREST extends AgentMiniClusterTestBase {


  @Test
  public void testStandaloneAgentWeb() throws Throwable {

    describe "create a standalone AM then perform actions on it"
    //launch fake master
    def conf = configuration
    conf.setBoolean(METRICS_LOGGING_ENABLED, true)
    conf.setInt(METRICS_LOGGING_LOG_INTERVAL, 1)
    String clustername = createMiniCluster("", conf, 1, true)


    ServiceLauncher<SliderClient> launcher =
        createStandaloneAMWithArgs(clustername,
            [Arguments.ARG_OPTION,
             RestTestDelegates.TEST_GLOBAL_OPTION, 
             RestTestDelegates.TEST_GLOBAL_OPTION_PRESENT],
            true, false)
    SliderClient client = launcher.service
    addToTeardown(client);

    ApplicationReport report = waitForClusterLive(client)
    def realappmaster = report.originalTrackingUrl

    // set up url config to match
    initConnectionFactory(launcher.configuration)


    execHttpRequest(WEB_STARTUP_TIME) {
      GET(realappmaster)
    }
    
    execHttpRequest(WEB_STARTUP_TIME) {
      def metrics = GET(realappmaster, SYSTEM_METRICS)
      log.info metrics
    }
    
    sleep(5000)
    def appmaster = report.trackingUrl

    GET(appmaster)

    log.info GET(appmaster, SYSTEM_PING)
    log.info GET(appmaster, SYSTEM_THREADS)
    log.info GET(appmaster, SYSTEM_HEALTHCHECK)
    log.info GET(appmaster, SYSTEM_METRICS_JSON)

    RestTestDelegates proxied = new RestTestDelegates(appmaster)
    RestTestDelegates direct = new RestTestDelegates(realappmaster)
    
    proxied.testCodahaleOperations()
    direct.testCodahaleOperations()

    describe "base entry lists"

    assertPathServesList(appmaster, LIVE, ApplicationResource.LIVE_ENTRIES)

    // now some REST gets
    describe "Application REST ${LIVE_RESOURCES}"
    proxied.testLiveResources()

    proxied.testRESTModel(appmaster)
    
    // PUT & POST &c must go direct for now
    direct.testPing(realappmaster)

  }


 
}
