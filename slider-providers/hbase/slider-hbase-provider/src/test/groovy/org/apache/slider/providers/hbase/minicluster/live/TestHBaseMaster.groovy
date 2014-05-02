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

package org.apache.slider.providers.hbase.minicluster.live

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.RoleKeys
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.providers.hbase.HBaseKeys
import org.apache.slider.core.registry.zk.ZKIntegration
import org.apache.slider.common.params.Arguments
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.slider.core.main.ServiceLaunchException
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

/**
 * Create a master against the File:// fs
 */
@CompileStatic
@Slf4j
class TestHBaseMaster extends HBaseMiniClusterTestBase {

  @Test
  public void testHBaseMaster() throws Throwable {
    String clustername = "test_hbase_master"
    createMiniCluster(clustername, getConfiguration(), 1, true)
    //make sure that ZK is up and running at the binding string
    ZKIntegration zki = createZKIntegrationInstance(ZKBinding, clustername, false, false, 5000)
    //now launch the cluster with 1 region server
    int regionServerCount = 1
    ServiceLauncher launcher = createHBaseCluster(clustername, regionServerCount,
      [
          Arguments.ARG_COMP_OPT, HBaseKeys.ROLE_MASTER, RoleKeys.JVM_HEAP, "256M",
          Arguments.ARG_DEFINE, SliderXmlConfKeys.KEY_YARN_QUEUE + "=default"
      ],
      true,
      true) 
    SliderClient sliderClient = (SliderClient) launcher.service
    addToTeardown(sliderClient);
    ClusterDescription status = sliderClient.getClusterDescription(clustername)
    
    //dumpFullHBaseConf(sliderClient, clustername)

    basicHBaseClusterStartupSequence(sliderClient)
    
    //verify the #of region servers is as expected
    dumpClusterStatus(sliderClient, "post-hbase-boot status")

    //get the hbase status
    waitForHBaseRegionServerCount(sliderClient, clustername, 1, hbaseClusterStartupToLiveTime)
    waitForWorkerInstanceCount(sliderClient, 1, hbaseClusterStartupToLiveTime)
    waitForRoleCount(sliderClient, HBaseKeys.ROLE_MASTER, 1,
                     hbaseClusterStartupToLiveTime)
  }

  @Test
  public void testHBaseMasterWithBadHeap() throws Throwable {
    String clustername = "test_hbase_master_with_bad_heap"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "verify that bad Java heap options are picked up"
    //now launch the cluster with 1 region server
    int regionServerCount = 1
    try {
      ServiceLauncher launcher = createHBaseCluster(clustername, regionServerCount,
        [Arguments.ARG_COMP_OPT, HBaseKeys.ROLE_WORKER, RoleKeys.JVM_HEAP, "invalid"], true, true) 
      SliderClient sliderClient = (SliderClient) launcher.service
      addToTeardown(sliderClient);

      AggregateConf launchedInstance = sliderClient.launchedInstanceDefinition
      AggregateConf liveInstance = sliderClient.launchedInstanceDefinition
      
      

      basicHBaseClusterStartupSequence(sliderClient)
      def report = waitForClusterLive(sliderClient)

      ClusterStatus clustat = getHBaseClusterStatus(sliderClient);
      // verify that region server cannot start
      if (clustat.servers.size()) {
        dumpClusterDescription("original",launchedInstance )
        dumpClusterDescription("live", sliderClient.liveInstanceDefinition)
        dumpClusterStatus(sliderClient,"JVM heap option not picked up")
      }
      assert 0 == clustat.servers.size()
    } catch (ServiceLaunchException e) {
      assertExceptionDetails(e, SliderExitCodes.EXIT_DEPLOYMENT_FAILED)
    }
  }
}
