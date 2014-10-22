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

package org.apache.slider.agent.actions

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.ActionExistsArgs
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Before
import org.junit.Test

/**
 * existence tests
 */
@CompileStatic
@Slf4j

class TestActionExists extends AgentMiniClusterTestBase {

  @Before
  public void setup() {
    super.setup()
    createMiniCluster("", configuration, 1, false)
  }
  
  @Test
  public void testExistsFailsWithUnknownCluster() throws Throwable {
    log.info("RM address = ${RMAddr}")
    try {
      ServiceLauncher launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
          SliderActions.ACTION_EXISTS,
          "unknown-cluster",
          Arguments.ARG_MANAGER, RMAddr
          ],
      )
      fail("expected an exception, got a status code "+ launcher.serviceExitCode)
    } catch (UnknownApplicationInstanceException e) {
      
    }
  }
    
  @Test
  public void testExistsLiveCluster() throws Throwable {
    //launch the cluster
    String clustername = createClusterName()
    ServiceLauncher<SliderClient> launcher = createStandaloneAM(
        clustername,
        true,
        false)
    SliderClient sliderClient = launcher.service
    addToTeardown(launcher)
    ApplicationReport report = waitForClusterLive(sliderClient)

    // exists holds when cluster is running
    launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
          SliderActions.ACTION_EXISTS,
          clustername,
          Arguments.ARG_MANAGER, RMAddr
          ],
      )
    assertSucceeded(launcher)

    //and when cluster is running
    launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
          SliderActions.ACTION_EXISTS,
          clustername,
          Arguments.ARG_LIVE,
          Arguments.ARG_MANAGER, RMAddr
          ],
      )

    assertSucceeded(launcher)
    
    // assert that the cluster exists
    assert 0 == sliderClient.actionExists(clustername, true)

    // assert that the cluster is in the running state
    ActionExistsArgs running = new ActionExistsArgs()
    running.state = YarnApplicationState.RUNNING.toString()
    assert 0 == sliderClient.actionExists(clustername, running)
    
    // stop the cluster
    clusterActionFreeze(sliderClient, clustername)

    //verify that exists(live) is now false
    assert LauncherExitCodes.EXIT_FALSE == sliderClient.actionExists(clustername, true)

    //but the cluster is still there for the default
    assert 0 == sliderClient.actionExists(clustername, false)
    assert LauncherExitCodes.EXIT_FALSE == sliderClient.actionExists(clustername, running)

    // verify that on a cluster thaw the existence probes still work, that is
    // they do not discover the previous instance and return false when there
    // is actually a live cluster
    ServiceLauncher launcher2 = thawCluster(clustername, [], true);
    addToTeardown(launcher2)
    assert 0 == sliderClient.actionExists(clustername, running)
    assert 0 == sliderClient.actionExists(clustername, true)
  }
  
}
