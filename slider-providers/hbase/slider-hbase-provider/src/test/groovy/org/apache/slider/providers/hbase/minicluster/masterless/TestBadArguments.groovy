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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.api.RoleKeys
import org.apache.slider.providers.hbase.HBaseKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.core.main.ServiceLaunchException
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

import static HBaseKeys.PROVIDER_HBASE
import static Arguments.ARG_PROVIDER

@CompileStatic
@Slf4j

class TestBadArguments extends HBaseMiniClusterTestBase {

  @Test
  public void testBadAMHeap() throws Throwable {
    String clustername = "test_bad_am_heap"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "verify that bad Java heap options are picked up"

    try {
      ServiceLauncher launcher = createCluster(clustername,
           [
               (HBaseKeys.ROLE_MASTER): 0,
               (HBaseKeys.ROLE_WORKER): 0,
           ],
           [
               Arguments.ARG_COMP_OPT, SliderKeys.COMPONENT_AM, RoleKeys.JVM_HEAP, "invalid",
               ARG_PROVIDER, PROVIDER_HBASE
           ],
           true,
           false,
           [:])
      SliderClient sliderClient = (SliderClient) launcher.service
      addToTeardown(sliderClient);

      ApplicationReport report = waitForClusterLive(sliderClient)
      assert report.yarnApplicationState == YarnApplicationState.FAILED
      
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(e, SliderExitCodes.EXIT_YARN_SERVICE_FAILED)
    }
    
  }

  /**
   * Test disabled because YARN queues don't get validated in the mini cluster
   * @throws Throwable
   */
  public void DisabledtestBadYarnQueue() throws Throwable {
    String clustername = "test_bad_yarn_queue"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "verify that bad Java heap options are picked up"

    try {
      ServiceLauncher launcher = createCluster(clustername,
           [
               (HBaseKeys.ROLE_MASTER): 0,
               (HBaseKeys.ROLE_WORKER): 0,
           ],
           [
               Arguments.ARG_DEFINE,
               SliderXmlConfKeys.KEY_YARN_QUEUE + "=noqueue",
               ARG_PROVIDER, PROVIDER_HBASE
           ],
           true,
           false,
           [:])
      SliderClient sliderClient = (SliderClient) launcher.service
      addToTeardown(sliderClient);

      ApplicationReport report = waitForClusterLive(sliderClient)
      assert report.yarnApplicationState == YarnApplicationState.FAILED
      
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(e, SliderExitCodes.EXIT_YARN_SERVICE_FAILED)
    }
    
  }

}
