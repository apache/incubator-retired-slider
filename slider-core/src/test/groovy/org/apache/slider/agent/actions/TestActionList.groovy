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

import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Before
import org.junit.Test

/**
 * Test List operations
 */
@Slf4j

class TestActionList extends AgentMiniClusterTestBase {

  @Before
  public void setup() {
    super.setup()
    createMiniCluster("", configuration, 1, false)
  }

  /**
   * This is a test suite to run the tests against a single cluster instance
   * for faster test runs
   * @throws Throwable
   */

  @Test
  public void testSuite() throws Throwable {
    testListThisUserNoClusters()
    testListAllUsersNoClusters()
    testListLiveCluster()
    testListMissingCluster()
  }
  
  public void testListThisUserNoClusters() throws Throwable {
    log.info("RM address = ${RMAddr}")
    ServiceLauncher<SliderClient> launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            SliderActions.ACTION_LIST,
            Arguments.ARG_MANAGER, RMAddr
        ]
    )
    assert launcher.serviceExitCode == 0
  }
  
  public void testListAllUsersNoClusters() throws Throwable {
    log.info("RM address = ${RMAddr}")
    ServiceLauncher<SliderClient> launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            SliderActions.ACTION_LIST,
            Arguments.ARG_MANAGER, RMAddr,
        ]
    )
    assert launcher.serviceExitCode == 0
  }

  public void testListLiveCluster() throws Throwable {
    //launch the cluster
    String clustername = createClusterName()
    ServiceLauncher<SliderClient> launcher = createStandaloneAM(
        clustername,
        true,
        false)
    addToTeardown(launcher)
    //do the low level operations to get a better view of what is going on 
    SliderClient sliderClient = launcher.service
    waitForClusterLive(sliderClient)

    //now list
    launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            SliderActions.ACTION_LIST,
        ]
    )
    assert launcher.serviceExitCode == 0
    //now look for the explicit sevice
    

    def serviceRegistryClient = sliderClient.YARNRegistryClient
    ApplicationReport instance = serviceRegistryClient.findInstance(clustername)
    assert instance != null
    log.info(instance.toString())

    //now list with the named cluster
    launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            SliderActions.ACTION_LIST, clustername
        ]
    )

  }

  public void testListMissingCluster() throws Throwable {
    describe("exec the status command against an unknown cluster")

    ServiceLauncher<SliderClient> launcher
    try {
      launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_LIST,
              createClusterName()
          ]
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (UnknownApplicationInstanceException e) {
      //expected
    }
  }


}
