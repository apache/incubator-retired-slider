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

package org.apache.slider.providers.hbase.minicluster.masterless

import groovy.util.logging.Slf4j
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.core.exceptions.ErrorStrings
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.common.tools.SliderFileSystem
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.ActionEchoArgs
import org.apache.slider.common.params.CommonArgs
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
//@CompileStatic
@Slf4j

class TestDestroyMasterlessAM extends HBaseMiniClusterTestBase {

  @Test
  public void testDestroyMasterlessAM() throws Throwable {
    String clustername = "test_destroy_masterless_am"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "create a masterless AM, stop it, try to create" +
             "a second cluster with the same name, destroy it, try a third time"

    ServiceLauncher launcher1 = launchClientAgainstMiniMR(
        getConfiguration(),
        [
            CommonArgs.ACTION_DESTROY,
            "no-cluster-of-this-name",
            Arguments.ARG_FILESYSTEM, fsDefaultName,
        ])
    assert launcher1.serviceExitCode == 0



    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, true)
    SliderClient sliderClient = (SliderClient) launcher.service
    addToTeardown(sliderClient);

    SliderFileSystem sliderFileSystem = createSliderFileSystem()
    def hdfs = sliderFileSystem.fileSystem
    def instanceDir = sliderFileSystem.buildClusterDirPath(clustername)

    assertPathExists(
        hdfs,
        "cluster path not found",
        instanceDir)

    sliderFileSystem.locateInstanceDefinition(clustername)
    clusterActionFreeze(sliderClient, clustername,"stopping first cluster")
    waitForAppToFinish(sliderClient)

    
    describe "Warnings below are expected"
    
    //now try to create instance #2, and expect an in-use failure
    try {
      createMasterlessAM(clustername, 0, false, false)
      fail("expected a failure, got an AM")
    } catch (SliderException e) {
      assertExceptionDetails(e,
                             SliderExitCodes.EXIT_INSTANCE_EXISTS,
                             ErrorStrings.E_ALREADY_EXISTS)
    }

    describe "END EXPECTED WARNINGS"


    describe "destroying $clustername"
    //now: destroy it
    
    int exitCode = sliderClient.actionDestroy(clustername);
    assert 0 == exitCode

    describe "post destroy checks"
    sliderFileSystem.verifyDirectoryNonexistent(instanceDir)

    describe "thaw expected to fail"
    //expect thaw to now fail
    try {
      launcher = launch(SliderClient,
                        configuration,
                        [
                            CommonArgs.ACTION_THAW,
                            clustername,
                            Arguments.ARG_FILESYSTEM, fsDefaultName,
                            Arguments.ARG_MANAGER, RMAddr,
                        ])
      fail("expected an exception")
    } catch (UnknownApplicationInstanceException e) {
      //expected
    }

    describe "thaw completed, checking dir is still absent"
    sliderFileSystem.verifyDirectoryNonexistent(instanceDir)


    describe "recreating $clustername"

    //and create a new cluster
    launcher = createMasterlessAM(clustername, 0, false, true)
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
      int ec = cluster2.actionDestroy(clustername)
      fail("expected a failure from the destroy, got error code $ec")
    } catch (SliderException e) {
      assertFailureClusterInUse(e);
    }
    
    //and try to destroy a completely different cluster just for the fun of it
    assert 0 == sliderClient.actionDestroy("no-cluster-of-this-name")
  }


}
