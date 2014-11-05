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
import org.junit.Test

@CompileStatic
@Slf4j
public class AgentFailuresIT extends AgentCommandTestBase
implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  private static String COMMAND_LOGGER = "COMMAND_LOGGER"
  private static String APPLICATION_NAME = "one-container-fail-register"
  private static String APP_TEMPLATE2 =
    "../slider-core/src/test/app_packages/test_command_log/appConfig_fast_no_reg.json"

  @After
  public void destroyCluster() {
    cleanup(APPLICATION_NAME)
  }

  @Test
  public void testAgentFailRegistrationOnce() throws Throwable {
    if (!AGENTTESTS_ENABLED) {
      skip("Agent tests are not run.")
    }

    cleanup(APPLICATION_NAME)

    File launchReportFile = createTempJsonFile();
    SliderShell shell = createTemplatedSliderApplication(
        APPLICATION_NAME,
        APP_TEMPLATE2,
        APP_RESOURCE,
        [],
        launchReportFile)
    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)
    expectContainerRequestedCountReached(APPLICATION_NAME, COMMAND_LOGGER, 2,
        CONTAINER_LAUNCH_TIMEOUT)
    sleep(1000 * 20)
    assertAppRunning(appId)
    def cd = assertContainersLive(APPLICATION_NAME, COMMAND_LOGGER, 1)
    assert cd.statistics[COMMAND_LOGGER]["containers.requested"] >= 2
    assertAppRunning(appId)
  }

}
