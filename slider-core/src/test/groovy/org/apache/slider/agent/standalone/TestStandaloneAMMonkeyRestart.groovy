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
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.api.InternalKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

@CompileStatic
@Slf4j
class TestStandaloneAMMonkeyRestart extends AgentMiniClusterTestBase {

  @Test
  public void testStandaloneAMMonkeyRestart() throws Throwable {
    describe "Run a Standalone AM with the Chaos monkey set to kill it"
    // patch the configuration for AM restart
    int threshold = 2;
    YarnConfiguration conf = getRestartableConfiguration(threshold)

    String clustername = createMiniCluster("", conf, 1, true)
    ServiceLauncher<SliderClient> launcher =
        createStandaloneAMWithArgs(clustername,
            [
                Arguments.ARG_OPTION, InternalKeys.CHAOS_MONKEY_ENABLED, "true",
                Arguments.ARG_OPTION, InternalKeys.CHAOS_MONKEY_INTERVAL_SECONDS, "8",
                Arguments.ARG_OPTION, InternalKeys.CHAOS_MONKEY_PROBABILITY_AM_FAILURE, "7500",
            ],
            true,
            false)
    SliderClient sliderClient = launcher.service
    addToTeardown(sliderClient);

    ApplicationReport report
    report = waitForClusterLive(sliderClient, 30000)
    describe "Waiting for the cluster to fail"
    def finishedReport = waitForAppToFinish(sliderClient, 90000)
    log.info(finishedReport.diagnostics)
    assert finishedReport.currentApplicationAttemptId.attemptId == threshold
    assert YarnApplicationState.FAILED == finishedReport.yarnApplicationState  
    assert FinalApplicationStatus.FAILED == finishedReport.finalApplicationStatus
  }

  /**
   * Get a restartable configuration
   * @param restarts
   * @return
   */
  public YarnConfiguration getRestartableConfiguration(int restarts) {
    def conf = new YarnConfiguration(configuration)
    conf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, restarts)
    conf.setInt(SliderXmlConfKeys.KEY_AM_RESTART_LIMIT, restarts)
    conf
  }


}
