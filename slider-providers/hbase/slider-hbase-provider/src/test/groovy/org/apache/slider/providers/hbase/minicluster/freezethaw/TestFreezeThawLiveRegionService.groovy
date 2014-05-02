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
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.RoleKeys
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.common.params.Arguments
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@CompileStatic
@Slf4j
class TestFreezeThawLiveRegionService extends HBaseMiniClusterTestBase {

  @Test
  public void testFreezeThawLiveRegionService() throws Throwable {
    String clustername = "test_freeze_thaw_live_regionservice"
    int regionServerCount = 2
    createMiniCluster(clustername, getConfiguration(), 1, true)
    describe("Create a cluster, freeze it, thaw it and verify that it came back ")
    //use a smaller AM HEAP to include it in the test cycle
    ServiceLauncher launcher = createHBaseCluster(clustername, regionServerCount,
          [
              Arguments.ARG_COMP_OPT, SliderKeys.COMPONENT_AM, RoleKeys.JVM_HEAP, "96M",
          ],
                                                  true, true)
    SliderClient sliderClient = (SliderClient) launcher.service
    addToTeardown(sliderClient);
    ClusterDescription status = sliderClient.getClusterDescription(clustername)
    log.info("${status.toJsonString()}")

    ClusterStatus clustat = basicHBaseClusterStartupSequence(sliderClient)

    clustat = waitForHBaseRegionServerCount(sliderClient, clustername, regionServerCount,
                            hbaseClusterStartupToLiveTime)
    describe("Cluster status")
    log.info(hbaseStatusToString(clustat));
    

    //verify you can't start a new cluster with that name
    try {
      ServiceLauncher launcher3 = createHBaseCluster(clustername, regionServerCount, [], false, false)
      SliderClient cluster3 = launcher3.service as SliderClient
      fail("expected a failure, got ${cluster3}")
    } catch (SliderException e) {
      assert e.exitCode == SliderExitCodes.EXIT_APPLICATION_IN_USE;
    }
    
    
    clusterActionFreeze(sliderClient, clustername)
    killAllRegionServers();
    //now let's start the cluster up again
    ServiceLauncher launcher2 = thawCluster(clustername, [], true);
    SliderClient newCluster = launcher2.service as SliderClient
    basicHBaseClusterStartupSequence(newCluster)

    //get the hbase status
    waitForHBaseRegionServerCount(newCluster, clustername, regionServerCount,
                            hbaseClusterStartupToLiveTime)
    
    // finally, attempt to thaw it while it is running
    //now let's start the cluster up again
    try {
      ServiceLauncher launcher3 = thawCluster(clustername, [], true);
      SliderClient cluster3 = launcher3.service as SliderClient
      fail("expected a failure, got ${cluster3}")
    } catch (SliderException e) {
      assert e.exitCode == SliderExitCodes.EXIT_APPLICATION_IN_USE
    }
  }




}
