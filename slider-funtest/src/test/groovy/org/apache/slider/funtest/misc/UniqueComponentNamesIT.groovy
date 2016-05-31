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

package org.apache.slider.funtest.misc

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.api.ClusterDescription
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.ResourcePaths
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class UniqueComponentNamesIT extends AgentCommandTestBase
implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  private static String COMMAND_LOGGER = "COMMAND_LOGGER"
  private static String COMMAND_LOGGER1 = COMMAND_LOGGER + "1"
  private static String COMMAND_LOGGER2 = COMMAND_LOGGER + "2"
  private static String COMMAND_LOGGER3 = COMMAND_LOGGER + "3"
  private static String APPLICATION_NAME = "unique-component-names"
  private static String APP_RESOURCE = ResourcePaths.UNIQUE_COMPONENT_RESOURCES

  @Before
  public void prepareCluster() {
    setupCluster(APPLICATION_NAME)
  }

  @After
  public void destroyCluster() {
    cleanup(APPLICATION_NAME)
  }

  @Test
  public void testCreateFlex() throws Throwable {
    assumeAgentTestsEnabled()

    describe APPLICATION_NAME

    def path = buildClusterPath(APPLICATION_NAME)
    assert !clusterFS.exists(path)

    File launchReportFile = createTempJsonFile();
    SliderShell shell = createTemplatedSliderApplication(APPLICATION_NAME,
        APP_TEMPLATE,
        APP_RESOURCE,
        [],
        launchReportFile)
    logShell(shell)

    ensureYarnApplicationIsUp(launchReportFile)
    ensureApplicationIsUp(APPLICATION_NAME)

    ClusterDescription cd = execStatus(APPLICATION_NAME)

    assert 3 == cd.statistics.size()
    assert cd.statistics.keySet().containsAll([SliderKeys.COMPONENT_AM, COMMAND_LOGGER1, COMMAND_LOGGER2])

    slider(EXIT_SUCCESS,
        [
            ACTION_FLEX,
            APPLICATION_NAME,
            ARG_COMPONENT,
            COMMAND_LOGGER,
            "3"
        ])

    sleep(1000 * 10)

    status(0, APPLICATION_NAME)
    expectLiveContainerCountReached(APPLICATION_NAME, COMMAND_LOGGER3, 1,
        CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(APPLICATION_NAME, COMMAND_LOGGER2, 1,
      CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(APPLICATION_NAME, COMMAND_LOGGER1, 1,
      CONTAINER_LAUNCH_TIMEOUT)

    slider(EXIT_SUCCESS,
      [
        ACTION_FLEX,
        APPLICATION_NAME,
        ARG_COMPONENT,
        COMMAND_LOGGER,
        "1"
      ])

    sleep(1000 * 10)

    status(0, APPLICATION_NAME)
    expectLiveContainerCountReached(APPLICATION_NAME, COMMAND_LOGGER3, 0,
      CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(APPLICATION_NAME, COMMAND_LOGGER2, 0,
      CONTAINER_LAUNCH_TIMEOUT)
    expectLiveContainerCountReached(APPLICATION_NAME, COMMAND_LOGGER1, 1,
      CONTAINER_LAUNCH_TIMEOUT)
  }

}
