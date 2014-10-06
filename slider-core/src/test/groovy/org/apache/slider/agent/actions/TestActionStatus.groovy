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
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.api.ClusterDescription
import org.apache.slider.common.params.SliderActions
import org.apache.slider.core.exceptions.BadClusterStateException
import org.apache.slider.core.exceptions.ErrorStrings
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.common.params.Arguments
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.ActionStatusArgs
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Before
import org.junit.Test

/**
 * status operations
 */
@CompileStatic
@Slf4j
class TestActionStatus extends AgentMiniClusterTestBase {


  /**
   * This is a test suite to run the tests against a single cluster instance
   * for faster test runs
   * @throws Throwable
   */

  @Test
  public void testSuite() throws Throwable {
    super.setup()
    createMiniCluster("testactionstatus", configuration, 1, true)
    testStatusLiveCluster()
    testStatusMissingCluster()
  }

  public void testStatusMissingCluster() throws Throwable {
    describe("create exec the status command against an unknown cluster")

    try {
      ServiceLauncher<SliderClient> launcher = launchClientAgainstMiniMR(
          new YarnConfiguration(miniCluster.config),
          [
              SliderActions.ACTION_STATUS,
              "teststatusmissingcluster",
              Arguments.ARG_MANAGER, RMAddr
          ]
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (UnknownApplicationInstanceException e) {
      //expected
    }

  }
  
  public void testStatusLiveCluster() throws Throwable {
    describe("create a live cluster then exec the status command")
    String clustername = "teststatuslivecluster"
    
    //launch the cluster
    ServiceLauncher<SliderClient> launcher = createStandaloneAM(
        clustername,
        true,
        false)

    SliderClient sliderClient = launcher.service
    ApplicationReport report = waitForClusterLive(sliderClient)


    //now look for the explicit sevice

    ActionStatusArgs statusArgs = new ActionStatusArgs()
    int status = sliderClient.actionStatus(clustername, statusArgs)
    assert 0 == status

    //now exec the status command
    ServiceLauncher statusLauncher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            SliderActions.ACTION_STATUS,
            clustername,
            Arguments.ARG_MANAGER, RMAddr,
        ]
        
    )
    assert statusLauncher.serviceExitCode == 0

    //status to a file
    File tfile = new File("target/$clustername-status.json")
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
            SliderActions.ACTION_STATUS,
            clustername,
            Arguments.ARG_MANAGER, RMAddr,
            Arguments.ARG_OUTPUT, path
        ]
    )
    assert statusLauncher.serviceExitCode == 0
    ClusterDescription cd2 = new ClusterDescription();
    cd2.fromJson(text)
    
    clusterActionFreeze(sliderClient, clustername, "stopping first cluster")
    def finishedAppReport = waitForAppToFinish(sliderClient)
    assert finishedAppReport.finalApplicationStatus ==
           FinalApplicationStatus.SUCCEEDED

    //now expect the status to fail
    try {
      status = sliderClient.actionStatus(clustername, new ActionStatusArgs())
      fail("expected an exception, but got the status $status")
    } catch (BadClusterStateException e) {
      assertExceptionDetails(e, SliderExitCodes.EXIT_BAD_STATE,
          ErrorStrings.E_APPLICATION_NOT_RUNNING)
    }
    
  }


}
