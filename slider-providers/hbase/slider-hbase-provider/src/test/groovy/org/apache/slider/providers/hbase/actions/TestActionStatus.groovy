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
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.api.ClusterDescription
import org.apache.slider.core.exceptions.BadClusterStateException
import org.apache.slider.core.exceptions.ErrorStrings
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.common.params.Arguments
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.ActionStatusArgs
import org.apache.slider.common.params.ClientArgs
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Before
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
//@CompileStatic
@Slf4j
class TestActionStatus extends HBaseMiniClusterTestBase {

  @Before
  public void setup() {
    super.setup()
    createMiniCluster("test_action_status", configuration, 1, false)
  }

  @Test
  public void testStatusMissingCluster() throws Throwable {
    describe("create exec the status command against an unknown cluster")
    //launch fake master
    //launch the cluster
    //exec the status command
    try {
      ServiceLauncher launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              ClientArgs.ACTION_STATUS,
              "test_status_missing_cluster",
              Arguments.ARG_MANAGER, RMAddr
          ]
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (UnknownApplicationInstanceException e) {
      //expected
    }

  }
  
  @Test
  public void testStatusLiveCluster() throws Throwable {
    describe("create a live cluster then exec the status command")
    //launch fake master
    String clustername = "test_status_live_cluster"
    
    //launch the cluster
    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, false)

    ApplicationReport report = waitForClusterLive(launcher.service)

    //do the low level operations to get a better view of what is going on 
    SliderClient sliderClient = (SliderClient) launcher.service

    //now look for the explicit sevice

    ActionStatusArgs statusArgs = new ActionStatusArgs()
    int status = sliderClient.actionStatus(clustername, statusArgs)
    assert status == SliderExitCodes.EXIT_SUCCESS

    //now exec the status command
    ServiceLauncher statusLauncher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            ClientArgs.ACTION_STATUS,
            clustername,
            Arguments.ARG_MANAGER, RMAddr,
        ]
        
    )
    assert statusLauncher.serviceExitCode == 0

    //status to a file
    File tfile = new File("target/" + clustername + "/status.json")
    statusArgs.output = tfile.absolutePath
    sliderClient.actionStatus(clustername, statusArgs)
    def text = tfile.text
    ClusterDescription cd = new ClusterDescription();
    cd.fromJson(text)
    
    //status to a file via the command line :  bin/slider status cl1 --out file.json
    String path = "target/cluster.json"
    statusLauncher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            ClientArgs.ACTION_STATUS,
            clustername,
            Arguments.ARG_MANAGER, RMAddr,
            Arguments.ARG_OUTPUT, path
        ]
    )
    assert statusLauncher.serviceExitCode == 0
    tfile = new File(path)
    ClusterDescription cd2 = new ClusterDescription();
    cd2.fromJson(text)
    
    clusterActionFreeze(sliderClient, clustername, "stopping first cluster")
    waitForAppToFinish(sliderClient)

    //now expect the status to fail
    try {
      status = sliderClient.actionStatus(clustername, new ActionStatusArgs())
      fail("expected an exception, but got the status $status")
    } catch (BadClusterStateException e) {
      assert e.toString().contains(ErrorStrings.E_APPLICATION_NOT_RUNNING)
    }
  }


}
