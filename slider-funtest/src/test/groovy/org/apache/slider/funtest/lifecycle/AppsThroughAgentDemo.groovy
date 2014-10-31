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
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class AppsThroughAgentDemo extends AgentCommandTestBase
implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  private static String COMMAND_LOGGER = "COMMAND_LOGGER"
  private static String APPLICATION_NAME = "agent-demo"


  @Before
  public void prepareCluster() {
    setupCluster(APPLICATION_NAME)
  }

  @Test
  public void testCreateFlex() throws Throwable {
    assumeAgentTestsEnabled()

    cleanup(APPLICATION_NAME)
    File launchReportFile = createAppReportFile();

    SliderShell shell = createTemplatedSliderApplication(APPLICATION_NAME,
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
            APPLICATION_NAME,
            ARG_COMPONENT,
            COMMAND_LOGGER,
            "2"])

    // sleep till the new instance starts
    sleep(1000 * 10)

    slider(EXIT_SUCCESS,
        [ACTION_STATUS,
            APPLICATION_NAME])

    expectContainersLive(APPLICATION_NAME, COMMAND_LOGGER, 2)

    String amWebUrl = getInfoAmWebUrl(APPLICATION_NAME)
    log.info("Dumping data from AM Web URL");
    log.info(amWebUrl.toURL().text);

    ensureRegistryCallSucceeds(APPLICATION_NAME)

    // get log folders
    shell = slider(EXIT_SUCCESS,
        [
            ACTION_REGISTRY,
            ARG_NAME,
            APPLICATION_NAME,
            ARG_LISTEXP
        ])
    if(!containsString(shell, "container_log_dirs") ||
       !containsString(shell, "container_work_dirs")) {
      logShell(shell)
      assert fail("Should list default exports container_log_dirs or container_work_dirs")
    }

    // get log folders
    shell = slider(EXIT_SUCCESS,
        [
            ACTION_REGISTRY,
            ARG_NAME,
            APPLICATION_NAME,
            ARG_GETEXP,
            "container_log_dirs"
        ])
    if(!containsString(shell, "\"tag\" : \"COMMAND_LOGGER\"", 2)
    || !containsString(shell, "\"level\" : \"component\"", 2)) {
      logShell(shell)
      assert fail("Should list 2 entries for log folders")
    }

    // get log folders
    shell = slider(EXIT_SUCCESS,
        [
            ACTION_REGISTRY,
            ARG_NAME,
            APPLICATION_NAME,
            ARG_GETEXP,
            "container_work_dirs"
        ])
    if(!containsString(shell, "\"tag\" : \"COMMAND_LOGGER\"", 2)
    || !containsString(shell, "\"level\" : \"component\"", 2)) {
      logShell(shell)
      assert fail("Should list 2 entries for work folder")
    }

    // get cl-site config
    shell = slider(
        [
            ACTION_REGISTRY,
            ARG_NAME,
            APPLICATION_NAME,
            ARG_GETCONF,
            "cl-site",
            ARG_FORMAT,
            "json"
        ])

    for (int i = 0; i < 10; i++) {
      if (shell.getRet() != EXIT_SUCCESS) {
        println "Waiting for the cl-site to show up"
        sleep(1000 * 10)
        shell = slider(
            [
                ACTION_REGISTRY,
                ARG_NAME,
                APPLICATION_NAME,
                ARG_GETCONF,
                "cl-site",
                ARG_FORMAT,
                "json"])
      }
    }
    assert shell.getRet() == EXIT_SUCCESS, "cl-site should be retrieved"
    if (!containsString(shell, "\"pattern.for.test.to.verify\" : \"verify this pattern\"", 1)) {
      logShell(shell)
      assert fail("Should have exported cl-site")
    }

    assertAppRunning(appId)
  }
}
