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
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.ActionAMSuicideArgs
import org.apache.slider.common.params.ActionDiagnosticArgs
import org.apache.slider.common.params.Arguments
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

/**
 * kill an AM and verify it is restarted
 */
@CompileStatic
@Slf4j

class TestStandaloneAMRestart extends AgentMiniClusterTestBase {


  @Test
  public void testStandaloneAMRestart() throws Throwable {
    describe "kill a Standalone AM and verify that it restarts"
    // patch the configuration for AM restart
    YarnConfiguration conf = getRestartableConfiguration(5)

    int restartLimit = 3;
    String clustername = createMiniCluster("", conf, 1, true)
    ServiceLauncher<SliderClient> launcher =
        createStandaloneAMWithArgs(clustername,
            [
                Arguments.ARG_DEFINE,
                SliderXmlConfKeys.KEY_AM_RESTART_LIMIT + "=" + restartLimit
            ],
            true,
            false)
    SliderClient sliderClient = launcher.service
    addToTeardown(sliderClient);

    ApplicationReport report = waitForClusterLive(sliderClient)
    logReport(report)
    waitUntilClusterLive(sliderClient, 30000)


    def diagnosticArgs = new ActionDiagnosticArgs()
    diagnosticArgs.client = true
    diagnosticArgs.yarn = true
    sliderClient.actionDiagnostic(diagnosticArgs)

    int iteration = 1;
    killAMAndWaitForRestart(sliderClient, iteration, clustername)


    killAMAndWaitForRestart(sliderClient, iteration++, clustername)
    // app should be running here
    assert 0 == sliderClient.actionExists(clustername, true)

    // kill again & expect it to be considered a failure
    killAmAndWaitForDeath(sliderClient, iteration++, clustername)
    sleep(40000)

    report = sliderClient.applicationReport
    assert report.finalApplicationStatus == FinalApplicationStatus.FAILED

    logReport(report)
    describe("Kill worked, freezing again")
    assert 0 == clusterActionFreeze(sliderClient, clustername, "force", true)
    assert 0 == clusterActionFreeze(sliderClient, clustername, "force", true)
    assert 0 == clusterActionFreeze(sliderClient, clustername, "force", true)
    assert 0 == clusterActionFreeze(sliderClient, clustername, "force", true)
    assert 0 == clusterActionFreeze(sliderClient, clustername, "force", true)
  }

  /**
   * Kill an AM. take an iteration count for the message sent to the 
   * AM (hence its logs)
   * @param iteration
   * @param sliderClient
   * @param clustername
   * @return
   */
  public ActionAMSuicideArgs killAMAndWaitForRestart(
      SliderClient sliderClient, int iteration, String clustername) {
    ActionAMSuicideArgs args = killAmAndWaitForDeath(
        sliderClient,
        iteration,
        clustername)
    //give yarn some time to notice
    sleep(40000)
    waitUntilClusterLive(sliderClient, 40000)
    return args
  }

  public ActionAMSuicideArgs killAmAndWaitForDeath(
      SliderClient sliderClient,
      int iteration,
      String clustername) {
    ActionAMSuicideArgs args = new ActionAMSuicideArgs()
    args.waittime = 100
    args.exitcode = 1
    args.message = "kill AM iteration #$iteration"
    sliderClient.actionAmSuicide(clustername, args)
    waitWhileClusterLive(sliderClient);
    return args
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
