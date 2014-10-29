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
import org.apache.hadoop.registry.client.binding.RegistryUtils
import org.apache.hadoop.registry.client.types.Endpoint
import org.apache.hadoop.registry.client.types.ServiceRecord
import org.apache.slider.api.InternalKeys
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

import static org.apache.slider.core.registry.info.CustomRegistryConstants.*

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

    // create an AM which fails to launch within a second
    File launchReportFile = createAppReportFile();
    SliderShell shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [
            ARG_INTERNAL, InternalKeys.CHAOS_MONKEY_ENABLED, "true",
            ARG_INTERNAL, InternalKeys.CHAOS_MONKEY_INTERVAL_SECONDS, "1",
            ARG_INTERNAL, InternalKeys.CHAOS_MONKEY_PROBABILITY_AM_FAILURE, "100",
        ],
        launchReportFile)

    maybeLookupFromLaunchReport(launchReportFile)
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
