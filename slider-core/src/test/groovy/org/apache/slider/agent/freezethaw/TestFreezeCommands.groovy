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

package org.apache.slider.agent.freezethaw

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.test.YarnMiniClusterTestBase
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Slf4j

class TestFreezeCommands extends AgentMiniClusterTestBase {

  @Test
  public void testFreezeCommands() throws Throwable {
    YarnConfiguration conf = configuration
    String clustername = createMiniCluster("", conf, 1, 1, 1, true, false)

    describe "create a masterless AM, stop it, try to stop again"

    ServiceLauncher<SliderClient> launcher = createStandaloneAM(
        clustername
        ,
        true,
        true);
    addToTeardown(launcher.service);


    log.info("ListOp")
    assertSucceeded(execSliderCommand(conf,
        [SliderActions.ACTION_LIST, clustername]))

    log.info("First stop command");
    ServiceLauncher freezeCommand = execSliderCommand(conf,
        [
            SliderActions.ACTION_FREEZE, clustername,
            Arguments.ARG_WAIT, waitTimeArg
        ]);
    assertSucceeded(freezeCommand)

    log.info("Second stop command");

    ServiceLauncher<SliderClient> freeze2 = execSliderCommand(conf,
        [
            SliderActions.ACTION_FREEZE, clustername,
            Arguments.ARG_WAIT, waitTimeArg
        ]);
    assertSucceeded(freeze2)

    log.info("First Exists");

    //assert there is no running cluster
    try {
      ServiceLauncher<SliderClient> exists1 = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          [
              SliderActions.ACTION_EXISTS, clustername,
              Arguments.ARG_FILESYSTEM, fsDefaultName,
              Arguments.ARG_LIVE
          ],
      )
      assert 0 != exists1.serviceExitCode;
    } catch (SliderException e) {
      assert e.exitCode == LauncherExitCodes.EXIT_FALSE;
    }

    log.info("First Start");


    def commands = [
        SliderActions.ACTION_THAW, clustername,
        Arguments.ARG_WAIT, waitTimeArg,
        Arguments.ARG_FILESYSTEM, fsDefaultName
    ]
    commands.addAll(extraCLIArgs)

    ServiceLauncher thawCommand = execSliderCommand(conf, commands);
    assertSucceeded(thawCommand)
    assertSucceeded(execSliderCommand(conf,
        [SliderActions.ACTION_LIST, clustername, 
         Arguments.ARG_LIVE]))
    assertSucceeded(execSliderCommand(conf,
        [SliderActions.ACTION_EXISTS, clustername]))

    log.info("stop 3");

    ServiceLauncher<SliderClient> freeze3 = execSliderCommand(conf,
        [
            SliderActions.ACTION_FREEZE, clustername,
            Arguments.ARG_WAIT, waitTimeArg
        ]);
    assertSucceeded(freeze3)

    log.info("thaw2");
    ServiceLauncher<SliderClient> thaw2 = execSliderCommand(conf,
        commands);
    assert 0 == thaw2.serviceExitCode;
    assertSucceeded(thaw2)

    try {
      log.info("thaw3 - should fail");
      ServiceLauncher<SliderClient> thaw3 = execSliderCommand(conf,
          commands);
      assert 0 != thaw3.serviceExitCode;
    } catch (SliderException e) {
      YarnMiniClusterTestBase.assertFailureClusterInUse(e);
    }

    //destroy should fail

    log.info("destroy1");

    try {
      ServiceLauncher<SliderClient> destroy1 = execSliderCommand(conf,
          [
              SliderActions.ACTION_DESTROY, clustername,
              Arguments.ARG_FILESYSTEM, fsDefaultName, Arguments.ARG_FORCE
          ]);
      fail(
          "expected a failure from the destroy, got error code ${destroy1.serviceExitCode}");
    } catch (SliderException e) {
      assertFailureClusterInUse(e);
    }
    log.info("freeze4");

    //kill -19 the process to hang it, then force kill
    killAM(YarnMiniClusterTestBase.SIGSTOP)

    ServiceLauncher<SliderClient> freeze4 = execSliderCommand(conf,
        [
            SliderActions.ACTION_FREEZE, clustername,
            Arguments.ARG_FORCE,
            Arguments.ARG_WAIT, waitTimeArg,
        ]);
    assertSucceeded(freeze4)

    log.info("destroy2");
    ServiceLauncher<SliderClient> destroy2 = execSliderCommand(conf,
        [
            SliderActions.ACTION_DESTROY, clustername,
            Arguments.ARG_FILESYSTEM, fsDefaultName, Arguments.ARG_FORCE
        ]);
    assertSucceeded(destroy2)

  }


}
