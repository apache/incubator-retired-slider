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

package org.apache.slider.providers.hbase.funtest

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.RoleKeys
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.common.tools.ConfigHelper
import org.apache.slider.common.params.Arguments
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.HBaseConfigFileOptions
import org.apache.slider.providers.hbase.HBaseTestUtils
import org.apache.zookeeper.*
import org.junit.After
import org.junit.Before
import org.junit.Test

import static org.apache.slider.providers.hbase.HBaseKeys.ROLE_MASTER
import static org.apache.slider.providers.hbase.HBaseKeys.ROLE_WORKER

@CompileStatic
@Slf4j
public class TestFunctionalHBaseCluster extends HBaseCommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes {


  public static final String HBASE_HEAP = "96m"

  public String getClusterName() {
    return "test_functional_hbase_cluster"
  }

  public String getClusterZNode() {
    return "/yarnapps_slider_yarn_" + getClusterName();
  }

  @Before
  public void prepareCluster() {

    String quorumServers = SLIDER_CONFIG.get(SliderXmlConfKeys.REGISTRY_ZK_QUORUM, DEFAULT_SLIDER_ZK_HOSTS)
  
    ZooKeeper monitor = new ZooKeeper(quorumServers,
      1000, new Watcher(){
      @Override
      public void process(WatchedEvent watchedEvent) {
      }
    }, false)
    try {
      ZKUtil.deleteRecursive(monitor, getClusterZNode())
    } catch (KeeperException.NoNodeException ignored) {
      log.info(getClusterZNode() + " not there")
    }
    setupCluster(clusterName)
  }

  @After
  public void teardownCluster() {
    teardown(clusterName)
  }

  @Test
  public void testHBaseCreateCluster() throws Throwable {

    describe description

    int numWorkers = SLIDER_CONFIG.getInt(KEY_SLIDER_TEST_NUM_WORKERS,
        DEFAULT_SLIDER_NUM_WORKERS);

    def clusterpath = buildClusterPath(clusterName)
    assert !clusterFS.exists(clusterpath)
    Map<String, Integer> roleMap = createHBaseCluster(
        clusterName,
        1, numWorkers,
        [
            ARG_OPTION,
              HBaseConfigFileOptions.KEY_HBASE_MASTER_INFO_PORT,
            Integer.toString(masterPortAssignment),
            ARG_COMP_OPT, ROLE_MASTER, RoleKeys.JVM_HEAP, HBASE_HEAP,
            ARG_OPTION,
            HBaseConfigFileOptions.KEY_REGIONSERVER_PORT,
              Integer.toString(workerPortAssignment),
            ARG_COMP_OPT, ROLE_WORKER, RoleKeys.JVM_HEAP, HBASE_HEAP,
        ],
        [:]
    )

    //get a slider client against the cluster
    SliderClient sliderClient = bondToCluster(SLIDER_CONFIG, clusterName)
    ClusterDescription cd2 = sliderClient.getClusterDescription()
    assert clusterName == cd2.name

    log.info("Connected via Client {} with {} workers", sliderClient.toString(),
        numWorkers)

    //wait for the role counts to be reached
    waitForRoleCount(sliderClient, roleMap, HBASE_LAUNCH_WAIT_TIME)

    Configuration clientConf = HBaseTestUtils.createHBaseConfiguration(sliderClient)
    HBaseTestUtils.assertHBaseMasterFound(clientConf)
    HBaseTestUtils.waitForHBaseRegionServerCount(sliderClient, clusterName,
                                  numWorkers, HBASE_LAUNCH_WAIT_TIME)

    clusterLoadOperations(clusterName, clientConf, numWorkers, roleMap, cd2)
  }


  public String getDescription() {
    return "Create a working HBase cluster $clusterName"
  }

  /**
   * Override point for any cluster load operations
   * @param clientConf
   * @param numWorkers
   */
  public void clusterLoadOperations(
      String clustername,
      Configuration clientConf,
      int numWorkers,
      Map<String, Integer> roleMap,
      ClusterDescription cd) {

    log.info("Client Configuration = " + ConfigHelper.dumpConfigToString(clientConf))
  }

}
