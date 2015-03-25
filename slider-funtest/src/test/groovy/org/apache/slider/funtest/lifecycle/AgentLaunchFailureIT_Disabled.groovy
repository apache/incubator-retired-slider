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
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.server.appmaster.SliderAppMaster

import static org.apache.slider.api.InternalKeys.*
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class AgentLaunchFailureIT_Disabled extends AgentCommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {


  static String CLUSTER = "test-agent-launchfail"

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
  public void testAgentLaunchFailure() throws Throwable {
    describe("Create a failing cluster and validate failure logic")

    // verify no cluster
    assert 0 != exists(CLUSTER).ret
 
    // create an AM which fails to launch
    File launchReportFile = createTempJsonFile();
    createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [
            ARG_OPTION, CHAOS_MONKEY_ENABLED, "true",
            ARG_OPTION, CHAOS_MONKEY_INTERVAL_SECONDS, "60",
            ARG_OPTION, CHAOS_MONKEY_PROBABILITY_AM_LAUNCH_FAILURE, 
             Integer.toString(PROBABILITY_PERCENT_100),
            ARG_DEFINE, SliderXmlConfKeys.KEY_AM_RESTART_LIMIT + "=1",
            ARG_WAIT, "0"
        ],
        launchReportFile)

    assert launchReportFile.exists()
    assert launchReportFile.size() > 0
    def launchReport = maybeLoadAppReport(launchReportFile)
    assert launchReport;
    assert launchReport.applicationId;

    // spin expecting failure
    def appId = launchReport.applicationId
    sleep(5000)
    describe("Awaiting failure")
    try {
      ensureYarnApplicationIsUp(appId)
      /*
       Under certain scenarios the app reaches the RUNNING state and gets
       probed in that state as well. The timings and delays in this test
       and the delays between scheduling of ChaosKillAM action cannot
       ensure that the app will never reach RUNNING state. Since, the
       target of this test is to check that chaos monkey kills the app,
       calling this twice will ensure that the app reaches the FINISHED
       state if chaos monkey is doing its job. If the app reaches FINISHED
       state in the first call itself then this second call will never be
       made. If this second call succeeds as well then chaos monkey is not
       doing its job and the test should fail.
       */
      describe("await for app to fail to launch app is running: enter spin state")
      1..10.each {
        sleep(5000)
        ensureYarnApplicationIsUp(appId)
      }
      fail("application is up when it should have failed")
    } catch (AssertionError e) {
      if(!e.toString().contains(SliderAppMaster.E_TRIGGERED_LAUNCH_FAILURE)) {
        throw e;
      }
    }
    def sar = lookupApplication(appId)
    log.info(sar.toString())
    assert sar.diagnostics.contains(SliderAppMaster.E_TRIGGERED_LAUNCH_FAILURE)
  }
}
