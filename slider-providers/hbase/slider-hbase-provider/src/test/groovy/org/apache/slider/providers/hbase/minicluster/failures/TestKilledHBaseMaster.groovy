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

package org.apache.slider.providers.hbase.minicluster.failures

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.ServerName
import org.apache.slider.api.ClusterDescription
import org.apache.slider.providers.hbase.HBaseKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

/**
 * test create a live region service
 */
@CompileStatic
@Slf4j
class TestKilledHBaseMaster extends HBaseMiniClusterTestBase {

  @Test
  public void testKilledHBaseMaster() throws Throwable {
    String clustername = "test_killed_hbase_master"
    int regionServerCount = 1
    createMiniCluster(clustername, getConfiguration(), 1, 1, 1, true, true)
    describe("Kill the hbase master and expect a restart");

    //now launch the cluster
    ServiceLauncher launcher = createHBaseCluster(clustername, regionServerCount, [], true, true)
    SliderClient sliderClient = (SliderClient) launcher.service
    addToTeardown(sliderClient);
    ClusterDescription status = sliderClient.getClusterDescription(clustername)

    ClusterStatus clustat = basicHBaseClusterStartupSequence(sliderClient)


    status = waitForWorkerInstanceCount(sliderClient, regionServerCount, hbaseClusterStartupToLiveTime)
    //get the hbase status
    ClusterStatus hbaseStat = waitForHBaseRegionServerCount(sliderClient, clustername, regionServerCount, hbaseClusterStartupToLiveTime)
    ServerName master = hbaseStat.master
    log.info("HBase master providing status information at {}",
             hbaseStat.master)
    
    Configuration clientConf = createHBaseConfiguration(sliderClient)
    clientConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 10);
    killAllMasterServers();
    status = waitForRoleCount(
        sliderClient, HBaseKeys.ROLE_MASTER, 1, hbaseClusterStartupToLiveTime)
    hbaseStat = waitForHBaseRegionServerCount(
        sliderClient,
        clustername,
        regionServerCount,
        hbaseClusterStartupToLiveTime)

    ServerName master2 = hbaseStat.master
    log.info("HBase master providing status information again at {}",
             master2)
    assert master2 != master
  }


}
