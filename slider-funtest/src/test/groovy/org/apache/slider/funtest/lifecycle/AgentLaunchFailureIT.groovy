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
import static org.apache.slider.api.InternalKeys.*
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class AgentLaunchFailureIT extends AgentCommandTestBase
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
    File launchReportFile = createAppReportFile();
    SliderShell shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [
            ARG_OPTION, CHAOS_MONKEY_ENABLED, "true",
            ARG_OPTION, CHAOS_MONKEY_PROBABILITY_AM_LAUNCH_FAILURE, 
             Integer.toString(PROBABILITY_PERCENT_100),
        ],
        launchReportFile)

    shell.dumpOutput();
    assert launchReportFile.exists()
    assert launchReportFile.size() > 0
    def launchReport = maybeLoadAppReport(launchReportFile)
    assert launchReport;
    assert launchReport.applicationId;
    def report = maybeLookupFromLaunchReport(launchReportFile)
    assert report;
    ensureApplicationIsUp(CLUSTER)

    //stop
    freeze(0, CLUSTER,
        [
            ARG_FORCE,
            ARG_WAIT, Integer.toString(FREEZE_WAIT_TIME),
            ARG_MESSAGE, "final-shutdown"
        ])

    destroy(0, CLUSTER)

    //cluster now missing
    exists(EXIT_UNKNOWN_INSTANCE, CLUSTER)

  }
}
