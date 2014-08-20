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
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.RoleKeys
import org.apache.slider.core.registry.docstore.PublishedConfigSet
import org.apache.slider.core.registry.info.ServiceInstanceData
import org.apache.slider.core.registry.retrieve.RegistryRetriever
import org.apache.slider.providers.hbase.HBaseKeys
import org.apache.slider.core.zk.ZKIntegration
import org.apache.slider.common.params.Arguments
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.server.services.curator.CuratorServiceInstance
import org.apache.slider.server.services.registry.SliderRegistryService
import org.junit.Test

/**
 * Create a master against the File:// fs
 */
@CompileStatic
@Slf4j
class TestHBaseMaster extends HBaseMiniClusterTestBase {
  public static final String HBASE_SITE = HBaseKeys.HBASE_SITE_PUBLISHED_CONFIG;

  @Test
  public void testHBaseMaster() throws Throwable {
    String clustername = createMiniCluster("", configuration, 1, true)
    //make sure that ZK is up and running at the binding string
    ZKIntegration zki = createZKIntegrationInstance(ZKBinding, clustername, false, false, 5000)
    //now launch the cluster with 1 region server
    int regionServerCount = 1
    ServiceLauncher<SliderClient> launcher = createHBaseCluster(clustername, regionServerCount,
      [
          Arguments.ARG_COMP_OPT, HBaseKeys.ROLE_MASTER, RoleKeys.JVM_HEAP, "256M",
          Arguments.ARG_DEFINE, SliderXmlConfKeys.KEY_YARN_QUEUE + "=default"
      ],
      true,
      true) 
    SliderClient client = launcher.service
    addToTeardown(client);
    ClusterDescription status = client.getClusterDescription(clustername)
    
    //dumpFullHBaseConf(sliderClient, clustername)

    basicHBaseClusterStartupSequence(client)
    
    //verify the #of region servers is as expected
    dumpClusterStatus(client, "post-hbase-boot status")

    //get the hbase status
    waitForHBaseRegionServerCount(client, clustername, 1, hbaseClusterStartupToLiveTime)
    waitForWorkerInstanceCount(client, 1, hbaseClusterStartupToLiveTime)
    waitForRoleCount(client, HBaseKeys.ROLE_MASTER, 1,
                     hbaseClusterStartupToLiveTime)
    
    // look up the registry entries for HBase 
    describe "service registry names"
    SliderRegistryService registryService = client.registry
    def names = registryService.getServiceTypes();
    dumpRegistryServiceTypes(names)

    List<CuratorServiceInstance<ServiceInstanceData>> instances =
        client.listRegistryInstances();

    def hbaseInstances = registryService.findInstances( HBaseKeys.HBASE_SERVICE_TYPE, null)
    assert hbaseInstances.size() == 1
    def hbaseService = hbaseInstances[0]
    assert hbaseService
    def hbaseServiceData = hbaseService.payload
    log.info "HBase service 0 == $hbaseServiceData"
    assert hbaseServiceData.id 
    assert hbaseServiceData.serviceType == HBaseKeys.HBASE_SERVICE_TYPE

    hbaseInstances = registryService.findInstances(
        HBaseKeys.HBASE_SERVICE_TYPE,
        clustername)
    assert hbaseInstances.size() == 1
    def hbaseServiceData2 = hbaseInstances[0].payload
    assert hbaseServiceData == hbaseServiceData2

    RegistryRetriever retriever = new RegistryRetriever(hbaseServiceData)
    log.info retriever.toString()
    assert retriever.hasConfigurations(true)
    PublishedConfigSet externalConfSet = retriever.getConfigurations(true)
    dumpConfigurationSet(externalConfSet)
    assert externalConfSet[HBASE_SITE]


    def yarnSite = retriever.retrieveConfiguration(
        externalConfSet,
        HBASE_SITE,
        true)
    assert !yarnSite.empty
    def siteXML = yarnSite.asConfiguration()
  }

}
