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
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.apache.slider.server.appmaster.web.rest.RestPaths
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class AppsThroughAgentIT extends AgentCommandTestBase
implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  private static String COMMAND_LOGGER = "COMMAND_LOGGER"
  private static String APPLICATION_NAME = "apps-through-agent"

  @Before
  public void prepareCluster() {
    setupCluster(APPLICATION_NAME)
  }
  
  public String getApplicationName() {
    return APPLICATION_NAME
  }

  @After
  public void destroyCluster() {
    cleanup(APPLICATION_NAME)
  }

  @Test
  public void testCreateFlex() throws Throwable {
    assumeAgentTestsEnabled()

    def application = APPLICATION_NAME
    cleanup(application)
    File launchReportFile = createTempJsonFile();
    SliderShell shell = createTemplatedSliderApplication(application,
        APP_TEMPLATE,
        APP_RESOURCE,
        [],
        launchReportFile)
    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)

    //flex
    slider(EXIT_SUCCESS,
        [
            ACTION_FLEX,
            application,
            ARG_COMPONENT,
            COMMAND_LOGGER,
            "2"])

    // sleep till the new instance starts
    sleep(1000 * 10)

    status(0, application)
    expectLiveContainerCountReached(application, COMMAND_LOGGER, 2,
        CONTAINER_LAUNCH_TIMEOUT)

    String amWebUrl = getInfoAmWebUrl(application)
    log.info("Dumping data from AM Web URL");

    def appmasterURL = amWebUrl.toURL()
    log.info(appmasterURL.text);
    URL registryExportsURL = new URL(appmasterURL,
        RestPaths.SLIDER_PATH_PUBLISHER
            + "/" + RestPaths.SLIDER_EXPORTS)

    ensureRegistryCallSucceeds(application, registryExportsURL)
    assertAppRunning(appId)
    def outfile = tmpFile(".txt")

    def commands = [
        ACTION_REGISTRY,
        ARG_NAME,
        application,
        ARG_LISTEXP,
        ARG_OUTPUT,
        outfile.absolutePath
    ]

    awaitRegistryOutfileContains(outfile, commands, "container_log_dirs")
    awaitRegistryOutfileContains(outfile, commands, "container_work_dirs")

    // get log folders
    shell = slider(EXIT_SUCCESS,
        [
            ACTION_REGISTRY,
            ARG_NAME,
            application,
            ARG_GETEXP,
            "container_log_dirs"])

    assertOutputContains(shell, '"tag" : "COMMAND_LOGGER"', 2)
    assertOutputContains(shell, '"level" : "component"', 2)

    // get cl-site config

    def getconf = [
        ACTION_REGISTRY,
        ARG_NAME,
        application,
        ARG_GETCONF,
        "cl-site",
        ARG_FORMAT,
        "json"]

    def pattern = '"pattern.for.test.to.verify" : "verify this pattern"'

    repeatUntilSuccess("registry",
        this.&commandSucceeds,
        REGISTRY_STARTUP_TIMEOUT,
        PROBE_SLEEP_TIME,
        [
            text: "pattern",
            command: getconf
        ],
        true,
        "failed to find $pattern in output") {
      slider(0, getconf)
      assertOutputContains(shell, pattern)
    }

    assertAppRunning(appId)
  }

}
