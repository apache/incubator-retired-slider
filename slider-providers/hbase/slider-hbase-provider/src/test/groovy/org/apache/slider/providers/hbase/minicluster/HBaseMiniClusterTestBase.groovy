/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.slider.providers.hbase.minicluster

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hbase.client.HConnection
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.ClusterNode
import org.apache.slider.api.ResourceKeys
import org.apache.slider.providers.hbase.HBaseKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.providers.hbase.HBaseTestUtils
import org.apache.slider.test.YarnZKMiniClusterTestBase

import static org.apache.slider.common.params.Arguments.*
import static org.apache.slider.test.SliderTestUtils.*
import static org.apache.slider.common.SliderXMLConfKeysForTesting.*
import static HBaseKeys.*
/**
 * test base for all hbase clusters
 */
@CompileStatic
@Slf4j
public abstract class HBaseMiniClusterTestBase extends YarnZKMiniClusterTestBase {

  public int hbaseClusterStartupTime = hbaseLaunchWaitTime

  /**
   * The time to sleep before trying to talk to the HBase Master and
   * expect meaningful results.
   */
  public int hbaseClusterStartupToLiveTime = hbaseClusterStartupTime

  public static final String HREGION = "HRegion"
  public static final String HMASTER = "HMaster"
  public static final String HB_HEAP = "256"


  @Override
  public String getTestConfigurationPath() {
    return "src/main/resources/" + HBASE_CONF_RESOURCE;
  }

  @Override
  void setup() {
    super.setup()
    def testConf = getTestConfiguration()
    assumeBoolOption(testConf, KEY_TEST_HBASE_ENABLED, true)
    assumeArchiveDefined();
    assumeApplicationHome();
  }

  /**
   * Teardown kills region servers
   */
  @Override
  void teardown() {
    super.teardown();
    if (teardownKillall) {
      killAllRegionServers();
      killAllMasterServers();
    }
  }

  /**
   * Kill all the region servers
   * <code>
   *    jps -l | grep HRegion | awk '{print $1}' | kill -9
   *  </code>
   */
  public void killAllRegionServers() {
    killJavaProcesses(HREGION, SIGKILL);
  }

  /**
   * Kill all master servers
   */
  public void killAllMasterServers() {
    killJavaProcesses(HMASTER, SIGKILL);
  }

  /**
   * Stop all the region servers
   * <code>
   *    jps -l | grep HRegion | awk '{print $1}' | kill -19
   *  </code>
   */
  public void stopAllRegionServers() {
    killJavaProcesses(HREGION, SIGTERM);
  }


  public static void assertHBaseMasterNotStopped(SliderClient sliderClient,
                                          String clustername) {
    String[] nodes = sliderClient.listNodeUUIDsByRole(ROLE_MASTER);
    int masterNodeCount = nodes.length;
    assert masterNodeCount > 0;
    ClusterNode node = sliderClient.getNode(nodes[0]);
    if (node.state >= ClusterDescription.STATE_STOPPED) {
      //stopped, not what is wanted
      log.error("HBase master has stopped");
      log.error(node.toString());
      fail("HBase master has stopped " + node.diagnostics);
    }
  }
  
  /**
   * Create an (unshared) HConnection talking to the hbase service that
   * Slider should be running
   * @param sliderClient slider client
   * @param clustername the name of the Slider cluster
   * @return the connection
   */
  public static HConnection createHConnection(Configuration clientConf) {
    return HBaseTestUtils.createHConnection(clientConf)
  }

  /**
   * get a string representation of an HBase cluster status
   * @param status cluster status
   * @return a summary for printing
   */
  public static String hbaseStatusToString(ClusterStatus status) {
    return HBaseTestUtils.hbaseStatusToString(status)
  }

  public static ClusterStatus getHBaseClusterStatus(SliderClient sliderClient) {
    return HBaseTestUtils.getHBaseClusterStatus(sliderClient)
  }

  public String getApplicationHomeKey() {
    return KEY_TEST_HBASE_HOME
  }

  public String getArchiveKey() {
    return KEY_TEST_HBASE_TAR
  }

  /**
   * Create an HBase config to work with
   * @param sliderClient slider client
   * @param clustername cluster
   * @return an hbase config extended with the custom properties from the
   * cluster, including the binding to the HBase cluster
   */
  public static Configuration createHBaseConfiguration(SliderClient sliderClient) {
    return HBaseTestUtils.createHBaseConfiguration(sliderClient)
  }

  /**
   * Create a full cluster with a master & the requested no. of region servers
   * @param clustername cluster name
   * @param workers # of nodes
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher<SliderClient> createHBaseCluster(String clustername,
      int workers,
      List<String> extraArgs,
      boolean deleteExistingData,
      boolean blockUntilRunning) {
    def masters = 1
    return createHBaseCluster(
        clustername,
        masters,
        workers,
        extraArgs,
        deleteExistingData,
        blockUntilRunning)
  }

  /**
   * Create a full cluster with a master & the requested no. of region servers
   * @param clustername cluster name
   * @param masters #of masters
   * @param workers # of nodes
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher<SliderClient> createHBaseCluster(
      String clustername,
      int masters,
      int workers,
      List<String> extraArgs,
      boolean deleteExistingData,
      boolean blockUntilRunning) {
    Map<String, Integer> roles = [
        (ROLE_MASTER): masters,
        (ROLE_WORKER): workers,
    ];
    extraArgs << ARG_RES_COMP_OPT << ROLE_MASTER << ResourceKeys.YARN_MEMORY << YRAM
    extraArgs << ARG_RES_COMP_OPT << ROLE_WORKER << ResourceKeys.YARN_MEMORY << YRAM
    extraArgs << ARG_PROVIDER << PROVIDER_HBASE;
    
    return createCluster(clustername,
        roles,
        extraArgs,
        deleteExistingData,
        blockUntilRunning,
        [:])
  }

  /**
   * Create an AM without a master
   * @param clustername AM name
   * @param size # of nodes
   * @param deleteExistingData should any existing cluster data be deleted
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher<SliderClient> createMasterlessAM(String clustername, int size, boolean deleteExistingData, boolean blockUntilRunning) {
    Map<String, Integer> roles = [
        (ROLE_MASTER): 0,
        (ROLE_WORKER): size,
    ];
    return createCluster(clustername,
        roles,
        [
            ARG_PROVIDER, PROVIDER_HBASE
        ],
        deleteExistingData,
        blockUntilRunning,
        [:])
  }

  public ClusterStatus basicHBaseClusterStartupSequence(SliderClient sliderClient) {
    return HBaseTestUtils.basicHBaseClusterStartupSequence(sliderClient,
                                   hbaseClusterStartupTime,
                                   hbaseClusterStartupToLiveTime)
  }

  /**
   * Spin waiting for the RS count to match expected
   * @param sliderClient client
   * @param clustername cluster name
   * @param regionServerCount RS count
   * @param timeout timeout
   */
  public static ClusterStatus waitForHBaseRegionServerCount(SliderClient sliderClient,
                                                     String clustername,
                                                     int regionServerCount,
                                                     int timeout) {

    return HBaseTestUtils.waitForHBaseRegionServerCount(sliderClient,
                                                        clustername,
                                                        regionServerCount,
                                                        timeout)
  }

  public boolean flexHBaseClusterTestRun(
      String clustername,
      int masters,
      int masterFlexTarget,
      int workers,
      int flexTarget,
      boolean testHBaseAfter) {
    createMiniCluster(clustername, getConfiguration(),
                      1,
                      true);
    //now launch the cluster
    SliderClient sliderClient;
    ServiceLauncher<SliderClient> launcher = createCluster(clustername,
           [
               (ROLE_MASTER): masters,
               (ROLE_WORKER): workers,
           ],
           [
               ARG_RES_COMP_OPT , ROLE_MASTER, ResourceKeys.YARN_MEMORY, YRAM,
               ARG_RES_COMP_OPT , ROLE_WORKER, ResourceKeys.YARN_MEMORY, YRAM,
               ARG_PROVIDER , PROVIDER_HBASE
           ],
           true,
           true,
           [:]);
    sliderClient = launcher.service;
    try {
      basicHBaseClusterStartupSequence(sliderClient);

      describe("Waiting for initial worker count of $workers");

      //verify the #of roles is as expected
      //get the hbase status
      waitForWorkerInstanceCount(sliderClient, workers, hbaseClusterStartupToLiveTime);
      waitForSliderMasterCount(sliderClient, masters, hbaseClusterStartupToLiveTime);

      log.info("Slider worker count at $workers, waiting for region servers to match");
      waitForHBaseRegionServerCount(sliderClient, clustername, workers, hbaseClusterStartupToLiveTime);

      //now flex
      describe("Flexing  masters:$masters -> $masterFlexTarget ; workers $workers -> $flexTarget");
      boolean flexed;
      flexed = 0 == sliderClient.flex(clustername,
          [
              (ROLE_WORKER): flexTarget,
              (ROLE_MASTER): masterFlexTarget
          ]
      );
      waitForWorkerInstanceCount(sliderClient, flexTarget, hbaseClusterStartupToLiveTime);
      waitForSliderMasterCount(sliderClient, masterFlexTarget,
                             hbaseClusterStartupToLiveTime);

      if (testHBaseAfter) {
        waitForHBaseRegionServerCount(sliderClient, clustername, flexTarget,
                                      hbaseClusterStartupToLiveTime);
      }
      return flexed;
    } finally {
      maybeStopCluster(sliderClient, null, "end of flex test run");
    }

  }

  /**
   * Spin waiting for the Slider worker count to match expected
   * @param sliderClient client
   * @param desiredCount RS count
   * @param timeout timeout
   */
  public static ClusterDescription waitForWorkerInstanceCount(SliderClient sliderClient,
                                                   int desiredCount,
                                                   int timeout) {
    return waitForRoleCount(sliderClient, ROLE_WORKER, desiredCount, timeout)
  }
  
  public static ClusterDescription waitForSliderMasterCount(SliderClient sliderClient,
                                                   int desiredCount,
                                                   int timeout) {
    return waitForRoleCount(sliderClient, ROLE_MASTER, desiredCount, timeout)
  }


  /**
   * attempt to talk to the hbase master; expect a failure
   * @param clientConf client config
   */
  public void assertNoHBaseMaster(
      SliderClient sliderClient, Configuration clientConf) {
    HBaseTestUtils.assertNoHBaseMaster(sliderClient, clientConf)
  }
  
  /**
   * attempt to talk to the hbase master; expect success
   * @param clientConf client config
   */
  public void assertHBaseMasterFound(Configuration clientConf) {
    HBaseTestUtils.assertHBaseMasterFound(clientConf)
  }

}
