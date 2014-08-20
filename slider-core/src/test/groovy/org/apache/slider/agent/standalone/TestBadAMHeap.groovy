/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.agent.standalone

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.api.RoleKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.core.main.ServiceLaunchException
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

@CompileStatic
@Slf4j

class TestBadAMHeap extends AgentMiniClusterTestBase {

  @Test
  public void testBadAMHeap() throws Throwable {
    String clustername = createMiniCluster("", configuration, 1, true)

    describe "verify that bad Java heap options are picked up"

    try {
      ServiceLauncher<SliderClient> launcher =
          createStandaloneAMWithArgs(clustername,
              [
                  Arguments.ARG_COMP_OPT,
                  SliderKeys.COMPONENT_AM,
                  RoleKeys.JVM_HEAP, "invalid",
              ],
              true,
              false)
      SliderClient sliderClient = launcher.service
      addToTeardown(sliderClient);

      ApplicationReport report = waitForClusterLive(sliderClient)
      assert report.yarnApplicationState == YarnApplicationState.FAILED
      
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(e, SliderExitCodes.EXIT_YARN_SERVICE_FAILED)
    }
    
  }

}
