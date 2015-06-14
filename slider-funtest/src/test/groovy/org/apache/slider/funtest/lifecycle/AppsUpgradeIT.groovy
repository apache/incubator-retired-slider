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
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.api.StatusKeys
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Test

/**
 * These are the steps required for this Rolling Upgrade (RU) test -
 * - Install an app package
 * - Deploy/create an app instance
 * - Install a new version of app package
 *     currently same ver is used, needs to be upgraded once a strategy to check
 *     application version can be found. Until then using same version is fine.
 * - Run the Slider App RU runbook steps (- and + scenarios)
 * - Verify the expected statuses and container counts
 * - Note: This is a lengthy test (takes approx 3-4 mins). RU tests contain
 *         multiple steps and validations and is expected to take that long.
 */
@CompileStatic
@Slf4j
public class AppsUpgradeIT extends AgentCommandTestBase
  implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {
  private static String COMMAND_LOGGER = "COMMAND_LOGGER"
  private static String APPLICATION_NAME = "app-upgrade-happy-path"
  private static String APP_RESOURCE =
      "../slider-core/src/test/app_packages/test_command_log/resources.json"

  @After
  public void destroyCluster() {
    cleanup(APPLICATION_NAME)
  }

  @Test
  public void testUpgrade() throws Throwable {
    assumeAgentTestsEnabled()

    cleanup(APPLICATION_NAME)
    File launchReportFile = createTempJsonFile();
    SliderShell shell = createTemplatedSliderApplication(
        APPLICATION_NAME,
        APP_TEMPLATE,
        APP_RESOURCE,
        [],
        launchReportFile)
    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)

    expectContainerRequestedCountReached(APPLICATION_NAME, COMMAND_LOGGER, 1,
        CONTAINER_LAUNCH_TIMEOUT)
    assertContainersLive(APPLICATION_NAME, COMMAND_LOGGER, 1)

    // flex
    slider(EXIT_SUCCESS,
        [
            ACTION_FLEX,
            APPLICATION_NAME,
            ARG_COMPONENT,
            COMMAND_LOGGER,
            "3"
        ])

    // spin till the flexed instance starts
    ensureYarnApplicationIsUp(appId)
    expectContainerRequestedCountReached(APPLICATION_NAME, COMMAND_LOGGER, 3,
        CONTAINER_LAUNCH_TIMEOUT)

    // upgrade spec
    describe("Call upgrade spec - spec mismatch with current state")
    slider(EXIT_BAD_CONFIGURATION,
        [
            ACTION_UPGRADE,
            APPLICATION_NAME,
            ARG_TEMPLATE,
            APP_TEMPLATE,
            ARG_RESOURCES,
            APP_RESOURCE
        ])
    describe("Call upgrade spec - spec mismatch with current state, use --force")
    slider(EXIT_SUCCESS,
        [
            ACTION_UPGRADE,
            APPLICATION_NAME,
            ARG_TEMPLATE,
            APP_TEMPLATE,
            ARG_RESOURCES,
            APP_RESOURCE,
            ARG_FORCE
        ])

    // This is a very time constrained check, so disabling it. Catching the
    // app not in RUNNING state after AM restart, is like racing with RM.
//    describe("Check Slider AM goes down and then comes back up")
//    ensureApplicationNotInState(APPLICATION_NAME, YarnApplicationState.RUNNING)
    // Spin till the Slider AM is back up. Note: COMMAND_LOGGER
    // container count goes down to 1 here (due to spec change)
    ensureYarnApplicationIsUp(appId)
    describe("COMMAND_LOGGER container requested count should get reset to 0")
    expectContainerRequestedCountReached(APPLICATION_NAME, COMMAND_LOGGER, 0,
      CONTAINER_LAUNCH_TIMEOUT)
    describe("COMMAND_LOGGER container live count should still be 1")
    expectLiveContainerCountReached(APPLICATION_NAME, COMMAND_LOGGER, 1,
      CONTAINER_LAUNCH_TIMEOUT)

    describe("New AM is back up. Wait for 60 secs to let existing "
      + "COMMAND_LOGGER container to heartbeat back to the new AM.")
    sleep(1000 * 60)

    // run upgrade container commands
    describe("Call upgrade container - invalid container id")
    slider(EXIT_NOT_FOUND,
        [
            ACTION_UPGRADE,
            APPLICATION_NAME,
            ARG_CONTAINERS,
            "container_1_invalid"
        ])
    describe("Call upgrade container - invalid component name")
    slider(EXIT_NOT_FOUND,
        [
            ACTION_UPGRADE,
            APPLICATION_NAME,
            ARG_COMPONENTS,
            "component_invalid"
        ])
    describe("Call upgrade container - valid component name")
    slider(EXIT_SUCCESS,
        [
            ACTION_UPGRADE,
            APPLICATION_NAME,
            ARG_COMPONENTS,
            COMMAND_LOGGER
        ])

    // verify
    describe("COMMAND_LOGGER container failed count should reach 1")
    expectFailedContainerCountReached(APPLICATION_NAME, COMMAND_LOGGER, 1,
        CONTAINER_LAUNCH_TIMEOUT)
    describe("COMMAND_LOGGER container request count should reach 1")
    expectContainerRequestedCountReached(APPLICATION_NAME, COMMAND_LOGGER, 1,
        CONTAINER_LAUNCH_TIMEOUT)
    describe("COMMAND_LOGGER container live count should reach 1")
    expectLiveContainerCountReached(APPLICATION_NAME, COMMAND_LOGGER, 1,
      CONTAINER_LAUNCH_TIMEOUT)

    def cd = execStatus(APPLICATION_NAME)
    assert cd.statistics[COMMAND_LOGGER][
        StatusKeys.STATISTICS_CONTAINERS_LIVE] == 1
    // check liveness
    def liveness =  cd.liveness
    assert liveness.allRequestsSatisfied
    assert 0 == liveness.requestsOutstanding

    assertInYarnState(appId, YarnApplicationState.RUNNING)
  }

}
