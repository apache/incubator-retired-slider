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
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.ActionEchoArgs
import org.apache.slider.common.params.ActionDestroyArgs
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.common.tools.SliderFileSystem
import org.apache.slider.core.exceptions.ErrorStrings
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.core.exceptions.UsageException
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

/**
 * destroy a standalone AM
 */
@CompileStatic
@Slf4j

class TestStandaloneAMDestroy extends AgentMiniClusterTestBase {

  @Test
  public void testStandaloneAMDestroy() throws Throwable {
    skipOnWindows()
    
    String clustername = createMiniCluster("", configuration, 1, true)

    describe "create a Standalone AM, stop it, try to create" +
             "a second cluster with the same name, destroy it, try a third time"

    ServiceLauncher<SliderClient> launcher1 = launchClientAgainstMiniMR(
        configuration,
        [
            SliderActions.ACTION_DESTROY,
            "no-cluster-of-this-name",
            Arguments.ARG_FILESYSTEM, fsDefaultName,
        ])
    assert launcher1.serviceExitCode == 0

    // try to list it and expect failures
    try {
      launchClientAgainstMiniMR(
          configuration,
          [
              SliderActions.ACTION_LIST,
              "no-cluster-of-this-name",
              Arguments.ARG_LIVE
          ])
      fail("expected a failure")
    } catch (UnknownApplicationInstanceException e) {
      assertExceptionDetails(e,
          SliderExitCodes.EXIT_UNKNOWN_INSTANCE,
          ErrorStrings.E_UNKNOWN_INSTANCE)
    }
    
    ServiceLauncher<SliderClient> launcher = createStandaloneAM(
        clustername,
        true,
        true)
    SliderClient sliderClient = launcher.service
    addToTeardown(sliderClient);

    SliderFileSystem sliderFileSystem = createSliderFileSystem()
    def fs = sliderFileSystem.fileSystem
    def instanceDir = sliderFileSystem.buildClusterDirPath(clustername)

    assertPathExists(
        fs,
        "cluster path not found",
        instanceDir)

    sliderFileSystem.locateInstanceDefinition(clustername)
    clusterActionFreeze(sliderClient, clustername, "stopping first cluster")
    def finishedAppReport = waitForAppToFinish(sliderClient)
    assert finishedAppReport.finalApplicationStatus == FinalApplicationStatus.SUCCEEDED

    
    describe "Warnings below are expected"
    
    //now try to create instance #2, and expect an in-use failure
    try {
      SliderClient am = createStandaloneAM(clustername, false, false).service
      fail("expected a failure, got an AM: $am")
    } catch (SliderException e) {
      assertExceptionDetails(e,
                             SliderExitCodes.EXIT_INSTANCE_EXISTS,
                             ErrorStrings.E_ALREADY_EXISTS)
    }

    describe "END EXPECTED WARNINGS"

    describe "destroying $clustername"
    //now: destroy it without the --force option and
    //expect it to fail

    def destroyEx = launchExpectingException(SliderClient,
        configuration,
        "",
        [
            SliderActions.ACTION_DESTROY,
            clustername,
            Arguments.ARG_FILESYSTEM, fsDefaultName,
            Arguments.ARG_MANAGER, RMAddr,
        ])
    assert destroyEx instanceof UsageException

    // destroy again but with --force option
    describe "destroying $clustername --force"
    ActionDestroyArgs destroyArgs = new ActionDestroyArgs()
    destroyArgs.force = true
    int exitCode = sliderClient.actionDestroy(clustername, destroyArgs);
    assert 0 == exitCode
    sleep(1000)
    // twice, not expecting an error the second time
    exitCode = sliderClient.actionDestroy(clustername, destroyArgs);
    assert 0 == exitCode

    describe "post destroy checks"
    if (fs.exists(instanceDir)) {
      log.warn("Destroy operation did not delete $instanceDir")
      rigorousDelete(sliderFileSystem, instanceDir, 60000)
    }
    
    sliderFileSystem.verifyDirectoryNonexistent(instanceDir)

    // look up app report and verify exit code is good
    
    
    describe "start expected to fail"
    //expect start to now fail
    def ex = launchExpectingException(SliderClient,
        configuration,
        "",
        [
            SliderActions.ACTION_THAW,
            clustername,
            Arguments.ARG_FILESYSTEM, fsDefaultName,
            Arguments.ARG_MANAGER, RMAddr,
        ])
    assert ex instanceof UnknownApplicationInstanceException

    describe "start completed, checking dir is still absent"
    sliderFileSystem.verifyDirectoryNonexistent(instanceDir)


    describe "recreating $clustername"

    //and create a new cluster
    launcher = createStandaloneAM(clustername, false, true)
    SliderClient cluster2 = launcher.service

    // do an echo here of a large string
    // Hadoop RPC couldn't handle strings > 32K chars, this
    // check here allows us to be confident that large JSON Reports are handled
    StringBuilder sb = new StringBuilder()
    for (int i = 0; i < 65536; i++) {
      sb.append(Integer.toString(i, 16))
    }
    ActionEchoArgs args = new ActionEchoArgs();
    args.message = sb.toString();
    def echoed = cluster2.actionEcho(clustername, args)
    assert echoed == args.message
    log.info(
        "Successful echo of a text document ${echoed.size()} characters long")
    //try to destroy it while live
    try {
      int ec = cluster2.actionDestroy(clustername, destroyArgs)
      fail("expected a failure from the destroy, got error code $ec")
    } catch (SliderException e) {
      assertFailureClusterInUse(e);
    }
    
    //and try to destroy a completely different cluster just for the fun of it
    assert 0 == sliderClient.actionDestroy("no-cluster-of-this-name", destroyArgs)

    maybeStopCluster(cluster2, "", "Teardown at end of test case", false);
  }


}
