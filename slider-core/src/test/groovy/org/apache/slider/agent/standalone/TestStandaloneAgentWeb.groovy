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

package org.apache.slider.agent.standalone

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.server.appmaster.web.rest.RestPaths
import org.junit.Test

import static org.apache.slider.server.appmaster.management.MetricsKeys.*

@CompileStatic
@Slf4j
class TestStandaloneAgentWeb extends AgentMiniClusterTestBase {

  
  @Test
  public void testStandaloneAgentWeb() throws Throwable {

    describe "create a standalone AM then perform actions on it"
    //launch fake master
    def conf = configuration
    conf.setBoolean(METRICS_LOGGING_ENABLED, true)
    conf.setInt(METRICS_LOGGING_LOG_INTERVAL, 1)
    String clustername = createMiniCluster("", conf, 1, true)


    ServiceLauncher<SliderClient> launcher =
        createStandaloneAM(clustername, true, false)
    SliderClient client = launcher.service
    addToTeardown(client);

    ApplicationReport report = waitForClusterLive(client)
    def realappmaster = report.originalTrackingUrl
    GET(realappmaster)
    def metrics = GET(realappmaster, RestPaths.SYSTEM_METRICS)
    log.info metrics
    
    sleep(5000)
    def appmaster = report.trackingUrl

    GET(appmaster)

    log.info GET(appmaster, RestPaths.SYSTEM_PING)
    log.info GET(appmaster, RestPaths.SYSTEM_THREADS)
    log.info GET(appmaster, RestPaths.SYSTEM_HEALTHCHECK)
    log.info GET(appmaster, RestPaths.SYSTEM_METRICS_JSON)
    
    describe "Hadoop HTTP operations"
    // now switch to the Hadoop URL connection, with SPNEGO escalation
    getWebPage(conf, appmaster)
    getWebPage(conf, appmaster, RestPaths.SYSTEM_THREADS)
    getWebPage(conf, appmaster, RestPaths.SYSTEM_HEALTHCHECK)
    getWebPage(conf, appmaster, RestPaths.SYSTEM_METRICS_JSON)
    
    log.info getWebPage(conf, realappmaster, RestPaths.SYSTEM_METRICS_JSON)

    
  }


}
