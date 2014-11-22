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
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Assume
import org.junit.Test

/**
 * kill a standalone AM and verify it shuts down. This test
 * also sets the retry count to 1 to stop recreation attempts
 */
@CompileStatic
@Slf4j

class TestStandaloneAMKill extends AgentMiniClusterTestBase {

  @Test
  public void testKillStandaloneAM() throws Throwable {
    Assume.assumeTrue(kill_supported)
    String clustername = createMiniCluster("", configuration, 1, true)

    describe "kill a Standalone AM and verify that it shuts down"
    ServiceLauncher<SliderClient> launcher =
        createStandaloneAMWithArgs(clustername,
          [
              Arguments.ARG_OPTION, SliderXmlConfKeys.KEY_AM_RESTART_LIMIT, "1"
          ],
          true,
          false)
    SliderClient sliderClient = launcher.service
    addToTeardown(sliderClient);
    ApplicationReport report = waitForClusterLive(sliderClient)
    assert report.yarnApplicationState == YarnApplicationState.RUNNING

    describe("listing Java processes")
    lsJavaProcesses();
    describe("killing AM")
    assert 0 == killAM(SIGTERM);
    waitWhileClusterLive(sliderClient);
    //give yarn some time to notice
    sleep(10000)
    describe("final listing")
    lsJavaProcesses();
    report = sliderClient.applicationReport
    assert YarnApplicationState.FAILED == report.yarnApplicationState;
    clusterActionFreeze(sliderClient, clustername)
  }


}
