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

/**
 * SETUP FOR THE TEST
 * Create valid labels, red and blue [yarn rmadmin -addLabels red,blue]
 * Add nodes with label [yarn rmadmin -setNodeToLabels host1:blue]
 * Perform refresh queue [yarn rmadmin -refreshQueues]
 *
 * Create a queue with access to labels - these are changes to capacity scheduler configuration
 *   Add a queue in addition to default
 *       yarn.scheduler.capacity.root.queues=default,labeled
 *   Provide capacity, take out from default
 *       yarn.scheduler.capacity.root.labeled.capacity=80
 *       yarn.scheduler.capacity.root.default.capacity=20
 *   Provide standard queue specs
 *       yarn.scheduler.capacity.root.labeled.state=RUNNING
 *       yarn.scheduler.capacity.root.labeled.maximum-capacity=80
 *   Have queue access the label
 *       yarn.scheduler.capacity.root.labeled.labels=red,blue
 *
 * After specifying the new configuration call refresh [yarn rmadmin -refreshQueues]
 *
 * See resources_queue_labels.json for label configuration required for the test
 *   Label expression for slider-appmaster is also the default for all containers
 *   if they do not specify own label expressions
 *       "yarn.label.expression":"red"
 *
 */
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
    SliderShell shell = createTemplatedSliderApplication(APPLICATION_NAME,
        APP_TEMPLATE,
        APP_RESOURCE4,
        [ARG_QUEUE, TARGET_QUEUE]
    )

    logShell(shell)

    ensureApplicationIsUp(APPLICATION_NAME)

    repeatUntilTrue(
        this.&hasRequestedContainerCountExceeded,
        50,
        1000 * 10,
        [limit      : '1',
         role       : COMMAND_LOGGER,
         application: APPLICATION_NAME]);

    expectContainersLive(APPLICATION_NAME, COMMAND_LOGGER, 1)

    //flex
    slider(EXIT_SUCCESS,
        [
            ACTION_FLEX,
            APPLICATION_NAME,
            ARG_COMPONENT,
            COMMAND_LOGGER,
            "2"])

    // sleep till the new instance starts
    ensureApplicationIsUp(APPLICATION_NAME)

    repeatUntilTrue(
        this.&hasRequestedContainerCountExceeded,
        20,
        1000 * 10,
        [limit      : '3',
         role       : COMMAND_LOGGER,
         application: APPLICATION_NAME]);

    sleep(1000 * 20)
    def cd = execStatus(APPLICATION_NAME)
    assert cd.statistics[COMMAND_LOGGER]["containers.requested"] >= 3

    assert isApplicationUp(APPLICATION_NAME), 'App is not running.'
  }


}
