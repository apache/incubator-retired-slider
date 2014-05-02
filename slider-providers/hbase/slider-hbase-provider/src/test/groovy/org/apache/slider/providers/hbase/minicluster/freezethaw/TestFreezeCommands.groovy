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

package org.apache.slider.providers.hbase.minicluster.freezethaw

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Slf4j

class TestFreezeCommands extends HBaseMiniClusterTestBase {

  @Test
  public void testFreezeCommands() throws Throwable {
    String clustername = "test_freeze_commands"
    YarnConfiguration conf = getConfiguration()
    createMiniCluster(clustername, conf, 1, 1, 1, true, true)

    describe "create a masterless AM, freeze it, try to freeze again"

    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, true);
    addToTeardown(launcher.service as SliderClient);

    
    log.info("ListOp")
    assertSucceeded(execSliderCommand(conf,
              [SliderActions.ACTION_LIST,clustername]))
    
    log.info("First Freeze command");
    ServiceLauncher freezeCommand = execSliderCommand(conf,
                          [SliderActions.ACTION_FREEZE, clustername,
                            Arguments.ARG_WAIT, waitTimeArg]);
    assertSucceeded(freezeCommand)

    log.info("Second Freeze command");

    ServiceLauncher freeze2 = execSliderCommand(conf,
                                [
                                    SliderActions.ACTION_FREEZE, clustername,
                                    Arguments.ARG_WAIT, waitTimeArg
                                ]);
    assertSucceeded(freeze2)

    log.info("First Exists");

    //assert there is no running cluster
    try {
      ServiceLauncher exists1 = launchClientAgainstMiniMR(
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

    log.info("First Thaw");


    def commands = [
        SliderActions.ACTION_THAW, clustername,
        Arguments.ARG_WAIT, waitTimeArg,
        Arguments.ARG_FILESYSTEM, fsDefaultName
    ]
    commands.addAll(extraCLIArgs)
    
    ServiceLauncher thawCommand = execSliderCommand(conf, commands);
    assertSucceeded(thawCommand)
    assertSucceeded(execSliderCommand(conf,
                  [SliderActions.ACTION_LIST, clustername]))
    assertSucceeded(execSliderCommand(conf,
                  [SliderActions.ACTION_EXISTS, clustername]))

    log.info("Freeze 3");

    ServiceLauncher freeze3 = execSliderCommand(conf,
                [
                    SliderActions.ACTION_FREEZE, clustername,
                    Arguments.ARG_WAIT, waitTimeArg
                ]);
    assertSucceeded(freeze3)

    log.info("thaw2");
    ServiceLauncher thaw2 = execSliderCommand(conf,
        commands);
    assert 0 == thaw2.serviceExitCode;
    assertSucceeded(thaw2)

    try {
      log.info("thaw3 - should fail");
      ServiceLauncher thaw3 = execSliderCommand(conf,
          commands);
      assert 0 != thaw3.serviceExitCode;
    } catch (SliderException e) {
      assertFailureClusterInUse(e);
    }

    //destroy should fail

    log.info("destroy1");

    try {
      ServiceLauncher destroy1 = execSliderCommand(conf,
          [
              SliderActions.ACTION_DESTROY, clustername,
              Arguments.ARG_FILESYSTEM, fsDefaultName
          ]);
      fail(
          "expected a failure from the destroy, got error code ${destroy1.serviceExitCode}");
    } catch (SliderException e) {
      assertFailureClusterInUse(e);
    }
    log.info("freeze4");
    
    //kill -19 the process to hang it, then force kill
    killAM(SIGSTOP)

    ServiceLauncher freeze4 = execSliderCommand(conf,
                                              [
                                                  SliderActions.ACTION_FREEZE, clustername,
                                                  Arguments.ARG_FORCE,
                                                  Arguments.ARG_WAIT, waitTimeArg,
                                              ]);
    assertSucceeded(freeze4)

    log.info("destroy2");
    ServiceLauncher destroy2 = execSliderCommand(conf,
                                               [
                                                   SliderActions.ACTION_DESTROY, clustername,
                                                   Arguments.ARG_FILESYSTEM, fsDefaultName,
                                               ]);
    assertSucceeded(destroy2)

  }


}
