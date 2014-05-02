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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.core.main.LauncherExitCodes
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
@CompileStatic
@Slf4j

class TestActionExists extends HBaseMiniClusterTestBase {

  @Before
  public void setup() {
    super.setup()
    createMiniCluster("TestActionExists", getConfiguration(), 1, false)
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
    String clustername = "testExistsLiveCluster"
    ServiceLauncher launcher = createMasterlessAM(clustername, 0, true, false)
    SliderClient sliderClient = launcher.service
    addToTeardown(launcher)
    ApplicationReport report = waitForClusterLive((SliderClient) launcher.service)

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
    
    // freeze the cluster
    clusterActionFreeze(sliderClient, clustername)

    //verify that exists(live) is now false
    assert LauncherExitCodes.EXIT_FALSE == sliderClient.actionExists(clustername, true)

    //but the cluster is still there for the default
    assert 0 == sliderClient.actionExists(clustername, false)
  }
  
}
