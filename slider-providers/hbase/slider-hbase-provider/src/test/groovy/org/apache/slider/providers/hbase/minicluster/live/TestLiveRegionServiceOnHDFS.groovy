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
import org.apache.slider.api.ClusterDescription
import org.apache.slider.core.registry.zk.ZKIntegration
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

/**
 * test create a live region service
 */
@CompileStatic
@Slf4j
class TestLiveRegionServiceOnHDFS extends HBaseMiniClusterTestBase {

  @Test
  public void testLiveRegionServiceOnHDFS() throws Throwable {
    String clustername = "test_live_region_service_on_hdfs"
    int regionServerCount = 1
    createMiniCluster(clustername, getConfiguration(), 1, 1, 1, true, true)
    describe(" Create a single region service cluster");

    //make sure that ZK is up and running at the binding string
    ZKIntegration zki = createZKIntegrationInstance(ZKBinding, clustername, false, false, 5000)
    //now launch the cluster
    ServiceLauncher launcher = createHBaseCluster(clustername, regionServerCount, [], true, true)
    SliderClient sliderClient = (SliderClient) launcher.service
    addToTeardown(sliderClient);
    ClusterDescription status = sliderClient.getClusterDescription(clustername)
    log.info("${status.toJsonString()}")

//    dumpFullHBaseConf(sliderClient)

    log.info("Running basic HBase cluster startup sequence")
    ClusterStatus clustat = basicHBaseClusterStartupSequence(sliderClient)


    status = waitForWorkerInstanceCount(sliderClient, regionServerCount, hbaseClusterStartupToLiveTime)
    describe("Cluster status")
    log.info(prettyPrint(status.toJsonString()))
    //get the hbase status
    waitForHBaseRegionServerCount(sliderClient, clustername, regionServerCount, hbaseClusterStartupToLiveTime)

  }

}
