/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.funtest.hbase

import groovy.util.logging.Slf4j
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderKeys
import org.apache.slider.core.registry.docstore.PublishedConfiguration
import org.apache.slider.core.registry.info.ServiceInstanceData
import org.apache.slider.core.registry.retrieve.RegistryRetriever
import org.apache.slider.funtest.framework.SliderShell
import org.apache.slider.server.services.curator.CuratorServiceInstance
import org.junit.Test

@Slf4j
class HBaseBasicIT extends HBaseAgentCommandTestBase {

  @Override
  public String getClusterName() {
    return "test_hbase_basic"
  }

  @Test
  public void testHBaseClusterCreate() throws Throwable {

    describe getDescription()

    def path = buildClusterPath(getClusterName())
    assert !clusterFS.exists(path)

    SliderShell shell = slider(EXIT_SUCCESS,
      [
        ACTION_CREATE, getClusterName(),
        ARG_IMAGE, agentTarballPath.toString(),
        ARG_TEMPLATE, APP_TEMPLATE,
        ARG_RESOURCES, APP_RESOURCE
      ])

    logShell(shell)

    ensureApplicationIsUp(getClusterName())

    // must match the values in src/test/resources/resources.json
    Map<String, Integer> roleMap = [
      "HBASE_MASTER" : 1,
      "HBASE_REGIONSERVER" : 1
    ];

    //get a slider client against the cluster
    SliderClient sliderClient = bondToCluster(SLIDER_CONFIG, getClusterName())
    ClusterDescription cd = sliderClient.clusterDescription
    assert getClusterName() == cd.name

    log.info("Connected via Client {}", sliderClient.toString())

    //wait for the role counts to be reached
    waitForRoleCount(sliderClient, roleMap, HBASE_LAUNCH_WAIT_TIME)

    sleep(HBASE_GO_LIVE_TIME)

    clusterLoadOperations(cd, sliderClient)
  }


  public String getDescription() {
    return "Create a working HBase cluster $clusterName"
  }

  public static String getMonitorUrl(SliderClient sliderClient, String clusterName) {
    CuratorServiceInstance<ServiceInstanceData> instance =
      sliderClient.getRegistry().queryForInstance(SliderKeys.APP_TYPE, clusterName)
    ServiceInstanceData serviceInstanceData = instance.payload
    RegistryRetriever retriever = new RegistryRetriever(serviceInstanceData)
    PublishedConfiguration configuration = retriever.retrieveConfiguration(
      retriever.getConfigurations(true), "quicklinks", true)

    // must match name set in metainfo.xml
    String monitorUrl = configuration.entries.get("org.apache.slider.monitor")

    assertNotNull monitorUrl
    return monitorUrl
  }

  public static void checkMonitorPage(String monitorUrl) {
    String monitor = fetchWebPageWithoutError(monitorUrl);
    assume monitor != null, "Monitor page null"
    assume monitor.length() > 100, "Monitor page too short"
    assume monitor.contains("Table Name"), "Monitor page didn't contain expected text"
  }

  /**
   * Override point for any cluster load operations
   */
  public void clusterLoadOperations(ClusterDescription cd, SliderClient sliderClient) {
    String monitorUrl = getMonitorUrl(sliderClient, getClusterName())
    assert monitorUrl.startsWith("http://"), "Monitor URL didn't have expected protocol"
    checkMonitorPage(monitorUrl)
  }
}
