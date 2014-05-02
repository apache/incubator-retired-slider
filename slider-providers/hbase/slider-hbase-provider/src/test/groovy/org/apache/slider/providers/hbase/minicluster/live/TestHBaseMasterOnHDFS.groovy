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
class TestHBaseMasterOnHDFS extends HBaseMiniClusterTestBase {

  @Test
  public void testHBaseMasteOnHDFS() throws Throwable {
    String clustername = "test_hbase_master_on_hdfs"
    createMiniCluster(clustername, getConfiguration(), 1, 1, 1, true, true)
    log.info("HDFS is at $fsDefaultName")
    assert fsDefaultName.startsWith("hdfs://")
    ServiceLauncher launcher = createHBaseCluster(clustername, 1, [], true, true) 
    SliderClient sliderClient = (SliderClient) launcher.service
    addToTeardown(sliderClient);
    ClusterDescription status = sliderClient.getClusterDescription(clustername)
    log.info("Status $status")
    
    basicHBaseClusterStartupSequence(sliderClient)
    dumpClusterStatus(sliderClient, "post-hbase-boot status")

    //get the hbase status
    status = waitForWorkerInstanceCount(sliderClient, 1, hbaseClusterStartupToLiveTime)
    waitForHBaseRegionServerCount(sliderClient, clustername, 1, hbaseClusterStartupToLiveTime)

    clusterActionFreeze(sliderClient, clustername)

  }


}
