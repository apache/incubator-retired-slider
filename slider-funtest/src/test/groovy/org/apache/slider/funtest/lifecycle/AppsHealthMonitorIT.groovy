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

import org.apache.curator.utils.EnsurePath
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.StatusKeys
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.ResourcePaths
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameter
import org.junit.runners.Parameterized.Parameters

import java.util.Arrays
import java.util.Collection

/**
 * These are the steps required for the Health Monitor tests -
 * - Install an app package
 * - Create an app A with 3 containers, 60% health threshold, 5 sec health
 *   window, 2 secs poll frequency, and 1 secs init delay. Node failure
 *   threshold is kept high at 1000 to prevent it to interfere with these tests.
 * - Create another app B with lots of containers (10K say), which will
 *   potentially eat up all the remaining resource in the default queue. Note,
 *   the idea is, that YARN will not be able to fulfil all the 10K container
 *   requests and hence a bunch of requests will be in Outstanding state.
 * - Then test the following scenarios:
 *   > Kill one container of the app A. YARN will immediately allocate a
 *     container to app B since it had Outstanding container requests ahead of
 *     app A. So YARN will not be able to satisfy the one container request for
 *     app A. Health of app A will come down to 66.67%, but it should continue
 *     to run beyond the health window (5 secs) expiry, since it is above the
 *     threshold of 60%.
 *
 * - Create an app A with 3 containers, 80% health threshold, 5 sec health
 *   window, 2 secs poll frequency, and 1 secs init delay. Node failure
 *   threshold is kept high at 1000 to prevent it to interfere with these tests.
 * - Create app B with same specs as the previous test
 * - Then test the following scenarios:
 *   > Kill one container of the app A. YARN will immediately allocate a
 *     container to app B since it had Outstanding container requests ahead of
 *     app A. So YARN will not be able to satisfy the one container request for
 *     app A. Health of app A will come down to 66.67%, so after the health
 *     window (5 secs) expiry it should be killed, since it is below threshold
 *     of 80%.
 *
 * - Create an app A with 3 containers, 80% health threshold, 5 sec health
 *   window, 2 secs poll frequency, and 1 secs init delay. Node failure
 *   threshold is kept high at 1000 to prevent it to interfere with these tests.
 * - Create app B with same specs as the previous test
 * - Then test the following scenarios:
 *   > Kill one container of the app A. YARN will immediately allocate a
 *     container to app B since it had Outstanding container requests ahead of
 *     app A. So YARN will not be able to satisfy the one container request for
 *     app A. Health of app A will come down to 66.67%, so if the health
 *     window (5 secs) expires it will be killed as proven by the previous test.
 *     However in this test before the window expires we do a flex down of the
 *     role which brings the total containers required to 2 and hence current
 *     health goes back to 100%. As a result app A does not killed and continues
 *     to run even beyond the health window expiry.
 *
 * - Repeat all the above 3 test scenarios but this time with app A having
 *   unique component names enabled. So a total of 6 unique tests are run in
 *   this suite.
 *
 * - Note: This is a lengthy test-suite. Each test takes approx 2-3 mins, so all
 *         6 tests in this suite takes approx 12-15 mins to run. Health monitor
 *         needs multiple success and failure simulations with appropriate
 *         window for each step to expire and subsequently validated for
 *         expected results.
 */
@RunWith(Parameterized.class)
@CompileStatic
@Slf4j
public class AppsHealthMonitorIT extends AgentCommandTestBase
  implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {
  private static String COMMAND_LOGGER = "COMMAND_LOGGER"
  private static String APPLICATION_NAME_60 = "app-health-monitor-60"
  private static String APPLICATION_NAME_80 = "app-health-monitor-80"
  private static String APPLICATION_NAME_LOTS_OF_CONTAINERS =
    "app-health-monitor-lots-of-containers"
  private static String APP_RESOURCE_60 =
    ResourcePaths.COMMAND_LOG_RESOURCES_HEALTH_MONITOR_60
  private static String APP_RESOURCE_80 =
    ResourcePaths.COMMAND_LOG_RESOURCES_HEALTH_MONITOR_80
  private static String APP_RESOURCE_UNIQUE_NAMES_60 =
    ResourcePaths.COMMAND_LOG_RESOURCES_HEALTH_MONITOR_UNIQUE_NAMES_60
  private static String APP_RESOURCE_UNIQUE_NAMES_80 =
    ResourcePaths.COMMAND_LOG_RESOURCES_HEALTH_MONITOR_UNIQUE_NAMES_80
  private static String APP_RESOURCE_LOTS_OF_CONTAINERS =
    ResourcePaths.COMMAND_LOG_RESOURCES_HEALTH_MONITOR_LOTS_OF_CONTAINERS

  @Parameter
  public Boolean isUniqueComp
  @Parameter(1)
  public String appResourceFor60
  @Parameter(2)
  public String appResourceFor80

  @Parameters
  public static Collection<Object[]> data() {
    Object[] testRun1 = [Boolean.FALSE, APP_RESOURCE_60, APP_RESOURCE_80]
    Object[] testRun2 = [Boolean.TRUE, APP_RESOURCE_UNIQUE_NAMES_60,
                         APP_RESOURCE_UNIQUE_NAMES_80]
    Object[][] data = [testRun1, testRun2]
    return Arrays.asList(data);
  }

  @After
  public void destroyCluster() {
    def appName60 = APPLICATION_NAME_60
    def appName80 = APPLICATION_NAME_80
    if (isUniqueComp) {
      appName60 += "-uniq-comp"
      appName80 += "-uniq-comp"
    }
    cleanup(appName60)
    cleanup(appName80)
    cleanup(APPLICATION_NAME_LOTS_OF_CONTAINERS)
  }

  @Test
  public void testHealthMonitorAppRunning() throws Throwable {
    describe("Running testHealthMonitorAppRunning for apps with resources "
        + appResourceFor60 + " and " + appResourceFor80 + " with unique comp = "
        + isUniqueComp)
    assumeAgentTestsEnabled()
    def appName = APPLICATION_NAME_60
    if (isUniqueComp) {
      appName += "-uniq-comp"
    }
    cleanup(appName)
    cleanup(APPLICATION_NAME_LOTS_OF_CONTAINERS)

    File launchReportFile = createTempJsonFile();
    SliderShell shell = createTemplatedSliderApplication(
        appName,
        APP_TEMPLATE,
        appResourceFor60,
        [],
        launchReportFile)
    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)
    if (isUniqueComp) {
      expectContainerRequestedCountReached(appName, COMMAND_LOGGER + "1", 1,
        CONTAINER_LAUNCH_TIMEOUT)
      assertContainersLive(appName, COMMAND_LOGGER + "1", 1)
      assertContainersLive(appName, COMMAND_LOGGER + "2", 1)
      assertContainersLive(appName, COMMAND_LOGGER + "3", 1)
    } else {
      expectContainerRequestedCountReached(appName, COMMAND_LOGGER, 3,
        CONTAINER_LAUNCH_TIMEOUT)
      assertContainersLive(appName, COMMAND_LOGGER, 3)
    }

    // Wait for 2 secs to get past the init delay and let the health monitor
    // polling to start
    describe("Wait for 2 secs to let the health monitor polling to start")
    sleep(1000 * 2)

    // Now bring up an app which will eat up all the remaining resources of the
    // default queue of the cluster and ensure it is up and running. Currently
    // it has 10,000 containers which is about 1.28TB of memory. Note, if this
    // test is executed in a queue with more than 1.28TB of memory (very slim
    // chance), then it will very likely fail.
    File launchReportFileLotsOfContainers = createTempJsonFile();
    shell = createTemplatedSliderApplication(
        APPLICATION_NAME_LOTS_OF_CONTAINERS,
        APP_TEMPLATE,
        APP_RESOURCE_LOTS_OF_CONTAINERS,
        [],
        launchReportFileLotsOfContainers)
    logShell(shell)

    def appIdLotsOfContainers =
        ensureYarnApplicationIsUp(launchReportFileLotsOfContainers)
    // Wait for 10 secs to let the containers come up (until no more resource is
    // left in the default queue)
    describe("Wait 10 secs to let containers come up and eat up all the memory")
    sleep(1000 * 10)

    // kill one container which will bring health down to about 66.67% but app
    // should continue to run, since threshold is 60%
    ClusterDescription cd = execStatus(appName)
    String containerId;
    if (isUniqueComp) {
      containerId = cd.instances.get(COMMAND_LOGGER + "3").get(0)
    } else {
      containerId = cd.instances.get(COMMAND_LOGGER).get(0)
    }
    describe("Killing container " + containerId)
    killContainer(appName, containerId)

    describe("Wait for 10 secs to ensure no container was allocated even after "
        + "expiry of health window, but then the app should continue to run")
    sleep(1000 * 10)
    ensureYarnApplicationIsUp(appId)
    // Also assert that only 2 containers are running
    if (isUniqueComp) {
      assertContainersLive(appName, COMMAND_LOGGER + "1", 1)
      assertContainersLive(appName, COMMAND_LOGGER + "2", 1)
      assertContainersLive(appName, COMMAND_LOGGER + "3", 0)
    } else {
      assertContainersLive(appName, COMMAND_LOGGER, 2)
    }
  }

  @Test
  public void testHealthMonitorAppStopped() throws Throwable {
    describe("Running testHealthMonitorAppStopped for apps with resources "
        + appResourceFor60 + " and " + appResourceFor80 + " with unique comp = "
        + isUniqueComp)
    assumeAgentTestsEnabled()
    def appName = APPLICATION_NAME_80
    if (isUniqueComp) {
      appName += "-uniq-comp"
    }
    cleanup(appName)
    cleanup(APPLICATION_NAME_LOTS_OF_CONTAINERS)

    File launchReportFile = createTempJsonFile();
    SliderShell shell = createTemplatedSliderApplication(
        appName,
        APP_TEMPLATE,
        appResourceFor80,
        [],
        launchReportFile)
    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)
    if (isUniqueComp) {
      expectContainerRequestedCountReached(appName, COMMAND_LOGGER + "1", 1,
        CONTAINER_LAUNCH_TIMEOUT)
      assertContainersLive(appName, COMMAND_LOGGER + "1", 1)
      assertContainersLive(appName, COMMAND_LOGGER + "2", 1)
      assertContainersLive(appName, COMMAND_LOGGER + "3", 1)
    } else {
      expectContainerRequestedCountReached(appName, COMMAND_LOGGER, 3,
        CONTAINER_LAUNCH_TIMEOUT)
      assertContainersLive(appName, COMMAND_LOGGER, 3)
    }

    // Wait for 2 secs to get past the init delay and let the health monitor
    // polling to start
    describe("Wait for 2 secs to let the health monitor polling to start")
    sleep(1000 * 2)

    // Now bring up app B
    File launchReportFileLotsOfContainers = createTempJsonFile();
    shell = createTemplatedSliderApplication(
        APPLICATION_NAME_LOTS_OF_CONTAINERS,
        APP_TEMPLATE,
        APP_RESOURCE_LOTS_OF_CONTAINERS,
        [],
        launchReportFileLotsOfContainers)
    logShell(shell)

    def appIdLotsOfContainers =
      ensureYarnApplicationIsUp(launchReportFileLotsOfContainers)
    // Wait for 10 secs to let the containers come up (until no more resource is
    // left in the default queue)
    describe("Wait 10 secs to let containers come up and eat up all the memory")
    sleep(1000 * 10)

    // kill one container which will bring health down to about 66.67% and app
    // should be shutdown after health window expires, since threshold is 80%
    ClusterDescription cd = execStatus(appName)
    String containerId;
    if (isUniqueComp) {
      containerId = cd.instances.get(COMMAND_LOGGER + "3").get(0)
    } else {
      containerId = cd.instances.get(COMMAND_LOGGER).get(0)
    }
    describe("Killing container " + containerId)
    killContainer(appName, containerId)

    describe("Wait 10 secs to give sufficient time for the app to be stopped")
    sleep(1000 * 10)
    if (isApplicationUp(appName)) {
      fail("Application should have been shutdown, but is still running")
    }
  }

  @Test
  public void testHealthMonitorAppSavedByFlex() throws Throwable {
    describe("Running testHealthMonitorAppSavedByFlex for apps with resources "
        + appResourceFor60 + " and " + appResourceFor80 + " with unique comp = "
        + isUniqueComp)
    assumeAgentTestsEnabled()
    def appName = APPLICATION_NAME_80
    if (isUniqueComp) {
      appName += "-uniq-comp"
    }
    cleanup(appName)
    cleanup(APPLICATION_NAME_LOTS_OF_CONTAINERS)

    File launchReportFile = createTempJsonFile();
    SliderShell shell = createTemplatedSliderApplication(
        appName,
        APP_TEMPLATE,
        appResourceFor80,
        [],
        launchReportFile)
    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)
    if (isUniqueComp) {
        expectContainerRequestedCountReached(appName, COMMAND_LOGGER + "1", 1,
          CONTAINER_LAUNCH_TIMEOUT)
        assertContainersLive(appName, COMMAND_LOGGER + "1", 1)
        assertContainersLive(appName, COMMAND_LOGGER + "2", 1)
        assertContainersLive(appName, COMMAND_LOGGER + "3", 1)
    } else {
      expectContainerRequestedCountReached(appName, COMMAND_LOGGER, 3,
        CONTAINER_LAUNCH_TIMEOUT)
      assertContainersLive(appName, COMMAND_LOGGER, 3)
    }

    // Wait for 2 secs to get past the init delay and let the health monitor
    // polling to start
    describe("Wait for 2 secs to let the health monitor polling to start")
    sleep(1000 * 2)

    // Now bring up app B
    File launchReportFileLotsOfContainers = createTempJsonFile();
    shell = createTemplatedSliderApplication(
        APPLICATION_NAME_LOTS_OF_CONTAINERS,
        APP_TEMPLATE,
        APP_RESOURCE_LOTS_OF_CONTAINERS,
        [],
        launchReportFileLotsOfContainers)
    logShell(shell)

    def appIdLotsOfContainers =
      ensureYarnApplicationIsUp(launchReportFileLotsOfContainers)
    // Wait for 10 secs to let the containers come up (until no more resource is
    // left in the default queue)
    describe("Wait 10 secs to let containers come up and eat up all the memory")
    sleep(1000 * 10)

    // kill one container which will bring health down to about 66.67% and app
    // could be shutdown if health window expires, since threshold is 80%
    ClusterDescription cd = execStatus(appName)
    String containerId;
    if (isUniqueComp) {
      containerId = cd.instances.get(COMMAND_LOGGER + "3").get(0)
    } else {
      containerId = cd.instances.get(COMMAND_LOGGER).get(0)
    }
    describe("Killing container " + containerId)
    killContainer(appName, containerId)

    // Before the health window expires, let's do a flex down to bring the
    // health above threshold and prevent the app from being killed. Let's not
    // do any additional checks after the kill container.
    describe("Flexing down by 1 container")
    slider(EXIT_SUCCESS,
        [
            ACTION_FLEX,
            appName,
            ARG_COMPONENT,
            COMMAND_LOGGER,
            "-1"
        ])

    describe("Wait for 10 secs to give sufficient time for the health window "
        + "to expire, and the app should continue to run")
    sleep(1000 * 10)
    ensureYarnApplicationIsUp(appId)
    // Now assert that only 2 containers are running
    if (isUniqueComp) {
      // note, after flex down the role 3 does not even exist
      assertContainersLive(appName, COMMAND_LOGGER + "1", 1)
      assertContainersLive(appName, COMMAND_LOGGER + "2", 1)
    } else {
      assertContainersLive(appName, COMMAND_LOGGER, 2)
    }
  }
}
