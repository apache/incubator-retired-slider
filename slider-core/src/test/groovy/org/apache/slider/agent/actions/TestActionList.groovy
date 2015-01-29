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
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.ActionListArgs
import org.apache.slider.common.params.ActionThawArgs
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.core.exceptions.BadCommandArgumentsException
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
    createMiniCluster("", configuration, 1, true)
  }

  /**
   * This is a test suite to run the tests against a single cluster instance
   * for faster test runs
   * @throws Throwable
   */

  @Test
  public void testActionListSuite() throws Throwable {
    testListThisUserNoClusters()
    testListMissingCluster()
    testActionList()
  }

  public void testActionList() {
    String clustername = "testactionlist"
    ServiceLauncher<SliderClient> launcher = createStandaloneAM(
        clustername,
        true,
        true)
    addToTeardown(launcher)
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


    def serviceRegistryClient = sliderClient.yarnAppListClient
    ApplicationReport instance = serviceRegistryClient.findInstance(clustername)
    assert instance != null
    log.info(instance.toString())
    ApplicationId originalAppId = instance.applicationId;

    //now list with the named cluster
    launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            SliderActions.ACTION_LIST, clustername
        ]
    )

    describe "listing by state"
    //Listing only live instances
    assert sliderClient.actionList(clustername, new ActionListArgs(live: true)) == 0;
    assert sliderClient.actionList(clustername,
        new ActionListArgs(live: true, verbose:true)) == 0;

    // find the same via the low-level operations


    def instances = sliderClient.enumSliderInstances(false, null, null)
    assert instances.size() > 0
    def enumeratedInstance = instances[clustername]
    assert enumeratedInstance.name == clustername
    assert enumeratedInstance.path.toString().endsWith("/" + clustername)
    assert enumeratedInstance.applicationReport != null
    assert originalAppId == enumeratedInstance.applicationReport.applicationId
    assert enumeratedInstance.applicationReport.yarnApplicationState == YarnApplicationState.RUNNING

    instances = sliderClient.enumSliderInstances(true,
        YarnApplicationState.RUNNING, YarnApplicationState.RUNNING)
    assert instances[clustername]

    clusterActionFreeze(sliderClient, clustername, "stopping first cluster")
    waitForAppToFinish(sliderClient)


    try {
      // unknown yarn state
      int e = sliderClient.actionList(clustername,
          new ActionListArgs(state: "undefined"));
      fail("expected failure, got return code of $e")
    } catch (BadCommandArgumentsException expected) {

    }

    try {
      // state and --live options
      int e = sliderClient.actionList(clustername,
          new ActionListArgs(state: "running", live: true));
      fail("expected failure, got return code of $e")
    } catch (BadCommandArgumentsException expected) {
      // expected
    }

    //Listing only live instances but prints nothing since instance is frozen/stopped

    describe("after freeze")
    // listing finished will work
    assert 0 == sliderClient.actionList("",
        new ActionListArgs(state: YarnApplicationState.FINISHED.toString()));
    assert 0 == sliderClient.actionList(clustername,
        new ActionListArgs(state: YarnApplicationState.FINISHED.toString(),
            verbose: true));

    assert -1 == sliderClient.actionList("", new ActionListArgs(live: true));
    assert -1 == sliderClient.actionList(clustername,
        new ActionListArgs(live: true));

    assert -1 == sliderClient.actionList(clustername,
        new ActionListArgs(state: YarnApplicationState.RUNNING.toString()));

    assert -1 == sliderClient.actionList("",
        new ActionListArgs(state: YarnApplicationState.RUNNING.toString()));

    // now look for finished app state
    instances = sliderClient.enumSliderInstances(false, null, null)
    enumeratedInstance = instances[clustername]
    assert enumeratedInstance.applicationReport.yarnApplicationState ==
           YarnApplicationState.FINISHED
    // look for running apps, expect no match
    instances = sliderClient.enumSliderInstances(true,
        YarnApplicationState.ACCEPTED, YarnApplicationState.RUNNING)
    assert null == instances[clustername]

    // look for terminated apps, expect no match
    instances = sliderClient.enumSliderInstances(true,
        YarnApplicationState.FINISHED, YarnApplicationState.KILLED)
    assert instances[clustername]

    // thaw
    sliderClient.actionThaw(clustername, new ActionThawArgs());
    waitForClusterLive(sliderClient)

    describe("Post-thaw listing")
    assert 0 == sliderClient.actionList(clustername,
        new ActionListArgs(state: YarnApplicationState.RUNNING.toString()));

    //Listing only live instances
    assert 0 == sliderClient.actionList(clustername,
        new ActionListArgs(live: true));

    //Listing all the instance both history (previously freezed instance) and live
    assert 0 == sliderClient.actionList("", new ActionListArgs(live: true));

    // look for terminated apps, expect no match
    instances = sliderClient.enumSliderInstances(true,
        YarnApplicationState.RUNNING, YarnApplicationState.RUNNING)
    assert instances[clustername]
    def runningId = instances[clustername].applicationReport.applicationId
    assert runningId != originalAppId

    // stop again

    maybeStopCluster(sliderClient, "", "forced", true)
    assert 0 == sliderClient.actionList(clustername,
        new ActionListArgs(state: "killed"));

    // look for terminated apps, match
    instances = sliderClient.enumSliderInstances(true,
        YarnApplicationState.FINISHED, YarnApplicationState.KILLED)
    assert instances[clustername]

    // and verify the report picked up is the latest one
    def finishedInstance = instances[clustername]

    def finishedAppReport = finishedInstance.applicationReport
    assert runningId == finishedAppReport.applicationId
    // which was force killed
    assert YarnApplicationState.KILLED == finishedAppReport.yarnApplicationState
    
    // check that an enum for live apps fails
    assert 0 == sliderClient.enumSliderInstances(true,
        YarnApplicationState.RUNNING, YarnApplicationState.RUNNING).size()
    
    // check that an enum for non-live apps works
    assert 0 < sliderClient.enumSliderInstances(false,
        YarnApplicationState.RUNNING, YarnApplicationState.RUNNING).size()
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
  public void testListMissingCluster() throws Throwable {
    describe("exec the list command against an unknown cluster")

    ServiceLauncher<SliderClient> launcher
    try {
      launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_LIST,
              "no-instance"
          ]
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (UnknownApplicationInstanceException e) {
      //expected
    }
  }

}
