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
public class AppsThroughAgentQueueAndLabelsIT extends AgentCommandTestBase
implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  private static String COMMAND_LOGGER = "COMMAND_LOGGER"
  private static String APPLICATION_NAME = "happy-path-with-queue-labels"
  private static String TARGET_QUEUE = "labeled"
  private static String APP_RESOURCE4 =
      "../slider-core/src/test/app_packages/test_command_log/resources_queue_labels.json"

  @After
  public void destroyCluster() {
    cleanup(APPLICATION_NAME)
  }

  @Test
  public void testCreateWithQueueAndLabels() throws Throwable {
    assumeAgentTestsEnabled()
    assumeQueueNamedLabelDefined()
    assumeLabelsRedAndBlueAdded()

    cleanup(APPLICATION_NAME)
    SliderShell shell = slider(EXIT_SUCCESS,
        [
            ACTION_CREATE, APPLICATION_NAME,
            ARG_QUEUE, TARGET_QUEUE,
            ARG_TEMPLATE, APP_TEMPLATE,
            ARG_RESOURCES, APP_RESOURCE4
        ])

    logShell(shell)

    ensureApplicationIsUp(APPLICATION_NAME)

    repeatUntilTrue(this.&hasContainerCountExceeded, 15, 1000 * 10, ['arg1': '1']);

    shell = slider(EXIT_SUCCESS,
        [
            ACTION_STATUS,
            APPLICATION_NAME])
    assertComponentCount(COMMAND_LOGGER, 1, shell)

    //flex
    slider(EXIT_SUCCESS,
        [
            ACTION_FLEX,
            APPLICATION_NAME,
            ARG_COMPONENT,
            COMMAND_LOGGER,
            "2"])

    // sleep till the new instance starts
    repeatUntilTrue(this.&hasContainerCountExceeded, 15, 1000 * 10, ['arg1': '2']);

    shell = slider(EXIT_SUCCESS,
        [
            ACTION_STATUS,
            APPLICATION_NAME])
    assertComponentCount(COMMAND_LOGGER, 2, shell)

    assertSuccess(shell)
    assert isApplicationInState("RUNNING", APPLICATION_NAME), 'App is not running.'
  }

  boolean hasContainerCountExceeded(Map<String, String> args) {
    int expectedCount = args['arg1'].toInteger();
    SliderShell shell = slider(EXIT_SUCCESS,
        [
            ACTION_STATUS,
            APPLICATION_NAME])

    //logShell(shell)
    int instanceCount = getComponentCount(COMMAND_LOGGER, shell)
    log.info("Noticed instance count - " + instanceCount + " for " + COMMAND_LOGGER)
    if (instanceCount >= expectedCount) {
      return true
    }

    return false
  }
}
