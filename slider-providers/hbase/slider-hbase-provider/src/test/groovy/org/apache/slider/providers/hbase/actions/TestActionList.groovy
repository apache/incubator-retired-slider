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

package org.apache.slider.providers.hbase.actions

import groovy.util.logging.Slf4j
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Before
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@Slf4j

class TestActionList extends HBaseMiniClusterTestBase {

  @Before
  public void setup() {
    super.setup()
    createMiniCluster("testActionList", getConfiguration(), 1, false)
  }
  
  @Test
  public void testListThisUserNoClusters() throws Throwable {
    log.info("RM address = ${RMAddr}")
    ServiceLauncher launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            SliderActions.ACTION_LIST,
            Arguments.ARG_MANAGER, RMAddr
        ]
    )
    assert launcher.serviceExitCode == 0
    SliderClient sliderClient = (SliderClient) launcher.service
  }
  
  @Test
  public void testListAllUsersNoClusters() throws Throwable {
    log.info("RM address = ${RMAddr}")
    ServiceLauncher launcher = launchClientAgainstMiniMR(
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

  @Test
  public void testListLiveCluster() throws Throwable {
    //launch the cluster
    String clustername = "test_list_live_cluster"
    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, false)
    ApplicationReport report = waitForClusterLive((SliderClient) launcher.service)

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
    
    //do the low level operations to get a better view of what is going on 
    SliderClient sliderClient = launcher.service
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



  @Test
  public void testListMissingCluster() throws Throwable {
    describe("create exec the status command against an unknown cluster")
    //launch fake master
    //launch the cluster
    //exec the status command
    ServiceLauncher launcher
    try {
      launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_LIST,
              "testStatusMissingCluster"
          ]
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (UnknownApplicationInstanceException e) {
      //expected
    }
  }


}
