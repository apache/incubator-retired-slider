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
import org.apache.hadoop.registry.client.binding.RegistryUtils
import org.apache.hadoop.registry.client.types.Endpoint
import org.apache.hadoop.registry.client.types.ServiceRecord
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.apache.slider.server.appmaster.web.rest.RestPaths
import org.apache.slider.test.Outcome
import org.junit.After
import org.junit.Before
import org.junit.Test

import static org.apache.slider.core.registry.info.CustomRegistryConstants.*

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
    describe("Create a 0-role cluster and make web queries against it")
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();
    SliderShell shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [],
        launchReportFile)

    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)
    assert appId
    expectRootWebPageUp(appId, instanceLaunchTime)
    File liveReportFile = createTempJsonFile();

    lookup(appId,liveReportFile)
    def report = loadAppReport(liveReportFile)
    assert report.url

    def root = report.url
    log.info GET(root, RestPaths.SYSTEM_METRICS)
    GET(root, RestPaths.SYSTEM_THREADS)
    log.info GET(root, RestPaths.SYSTEM_HEALTHCHECK)
    log.info GET(root, RestPaths.SYSTEM_PING)
  }

}
