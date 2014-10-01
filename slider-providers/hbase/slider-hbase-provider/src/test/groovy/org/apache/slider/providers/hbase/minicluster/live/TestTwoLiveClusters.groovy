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
import org.apache.hadoop.yarn.registry.client.api.RegistryOperations
import org.apache.hadoop.yarn.registry.client.binding.RegistryUtils
import org.apache.hadoop.yarn.registry.client.binding.RegistryPathUtils
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.HBaseKeys
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

@CompileStatic
@Slf4j

class TestTwoLiveClusters extends HBaseMiniClusterTestBase {

  /**
   * Create two clusters simultaneously and verify that their lifecycle is
   * independent.
   * @throws Throwable
   */
  @Test
  public void testTwoLiveClusters() throws Throwable {
    String clustername = createMiniCluster("", configuration, 1, true)

    String clustername1 = clustername + "-1"
    //now launch the cluster
    int regionServerCount = 1
    ServiceLauncher<SliderClient> launcher = createHBaseCluster(clustername1, regionServerCount, [], true, true) 
    SliderClient sliderClient = launcher.service
    addToTeardown(sliderClient);

    basicHBaseClusterStartupSequence(sliderClient)
    
    //verify the #of region servers is as expected
    dumpClusterStatus(sliderClient, "post-hbase-boot status")

    //get the hbase status
    waitForWorkerInstanceCount(sliderClient, 1, hbaseClusterStartupToLiveTime)
    waitForHBaseRegionServerCount(sliderClient, clustername1, 1, hbaseClusterStartupToLiveTime)

    //now here comes cluster #2
    String clustername2 = clustername + "-2"


    String zkpath = "/$clustername2"
    launcher = createHBaseCluster(clustername2, regionServerCount,
                                 [
                                     Arguments.ARG_ZKPATH, zkpath
                                 ],
                                 true,
                                 true)
    SliderClient cluster2Client = launcher.service
    addToTeardown(cluster2Client);

    basicHBaseClusterStartupSequence(cluster2Client)
    waitForWorkerInstanceCount(cluster2Client, 1, hbaseClusterStartupToLiveTime)
    waitForHBaseRegionServerCount(cluster2Client, clustername2, 1, hbaseClusterStartupToLiveTime)

    //and now verify that cluster 1 is still happy
    waitForHBaseRegionServerCount(sliderClient, clustername1, 1, hbaseClusterStartupToLiveTime)

    // registry instances    def names = client.listRegistryNames(clustername)
    describe "service registry names"
    RegistryOperations registry = cluster2Client.registryOperations
    def home = RegistryUtils.homePathForCurrentUser()

    def userSliderInstancesPath = RegistryUtils.serviceclassPath(
        RegistryUtils.currentUser(), SliderKeys.APP_TYPE)
    
   
    def names = RegistryUtils.listServiceRecords(registry,
        userSliderInstancesPath)
    dumpMap(names)
    def statMap = RegistryUtils.statChildren(registry, userSliderInstancesPath)
    assert statMap.size() == 2
    List<String> instanceIds = sliderClient.listRegisteredSliderInstances()

    dumpRegistryInstanceIDs(instanceIds)
    assert names.size() == 2
    assert instanceIds.size() == 2


    Map<String, ServiceRecord> instances =
        sliderClient.listRegistryInstances()
    dumpRegistryInstances(instances)
    assert instances.size() == 2


    def hbaseServicePath = RegistryUtils.serviceclassPath(
        RegistryUtils.currentUser(),
        HBaseKeys.HBASE_SERVICE_TYPE)
    Map<String, ServiceRecord> hbaseInstances =
        RegistryUtils.listServiceRecords(registry,
            hbaseServicePath);
        
    assert hbaseInstances.size() == 2
    String clusterPath1 = RegistryPathUtils.join(hbaseServicePath, clustername1)
    String clusterPath2 = RegistryPathUtils.join(hbaseServicePath, clustername2)
    assert hbaseInstances[clusterPath1] != null
    assert hbaseInstances[clusterPath2] != null
    assert hbaseInstances[clusterPath1] != hbaseInstances[clusterPath2]

    clusterActionFreeze(cluster2Client, clustername2, "stop cluster 2")
    clusterActionFreeze(sliderClient, clustername1, "Stop cluster 1")

  }

}
