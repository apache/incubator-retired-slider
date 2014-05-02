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

package org.apache.slider.providers.hbase.minicluster.archives

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

/**
 * Test of RM creation. This is so the later test's prereq's can be met
 */
@CompileStatic
@Slf4j
class TestFreezeThawClusterFromArchive extends HBaseMiniClusterTestBase {


  @Test
  public void testFreezeThawClusterFromArchive() throws Throwable {
    String clustername = "test_freeze_thaw_cluster_from_archive"
    int regionServerCount = 1
    createMiniCluster(clustername, configuration, 1, true)
    switchToImageDeploy = true
    ServiceLauncher<SliderClient> launcher = createHBaseCluster(clustername, regionServerCount, [], true, true)
    SliderClient sliderClient = launcher.service
    ClusterDescription status = sliderClient.getClusterDescription(clustername)
    log.info("${status.toJsonString()}")

    ClusterStatus clustat = basicHBaseClusterStartupSequence(sliderClient)

    waitForHBaseRegionServerCount(sliderClient, clustername, regionServerCount,
                            hbaseClusterStartupToLiveTime)


    clusterActionFreeze(sliderClient, clustername, "test freeze")
    describe("Restarting cluster")
    killAllRegionServers();

    //now let's start the cluster up again
    ServiceLauncher launcher2 = thawCluster(clustername, [], true);
    SliderClient newCluster = launcher2.service
    basicHBaseClusterStartupSequence(newCluster)

    //get the hbase status
    waitForHBaseRegionServerCount(newCluster, clustername, regionServerCount,
                            hbaseClusterStartupToLiveTime)

  }




}
