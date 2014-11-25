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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.api.ClusterNode
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.ActionRegistryArgs
import org.apache.slider.common.tools.Duration
import org.apache.slider.core.build.InstanceBuilder
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.launch.LaunchedApplication
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.persist.LockAcquireFailedException
import org.apache.slider.core.registry.retrieve.AMWebClient
import org.apache.slider.server.appmaster.web.rest.RestPaths
import org.junit.After
import org.junit.Test

@CompileStatic
@Slf4j
class TestStandaloneAgentWeb extends AgentMiniClusterTestBase {

  
  @Test
  public void testStandaloneAgentWeb() throws Throwable {

    describe "create a standalone AM then perform actions on it"
    //launch fake master
    String clustername = createMiniCluster("", configuration, 1, true)


    ServiceLauncher<SliderClient> launcher =
        createStandaloneAM(clustername, true, false)
    SliderClient client = launcher.service
    addToTeardown(client);

    ApplicationReport report = waitForClusterLive(client)
    def realappmaster = report.originalTrackingUrl
    GET(realappmaster)
    def metrics = GET(realappmaster, RestPaths.SYSTEM_METRICS)
    log.info metrics

    def appmaster = report.trackingUrl

    GET(appmaster)

    log.info GET(appmaster, RestPaths.SYSTEM_PING)
    log.info GET(appmaster, RestPaths.SYSTEM_THREADS)
    log.info GET(appmaster, RestPaths.SYSTEM_HEALTHCHECK)
    
  }


}
