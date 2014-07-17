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
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.RoleKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.tools.ConfigHelper
import org.apache.slider.core.registry.info.RegistryNaming
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.providers.hbase.HBaseConfigFileOptions
import org.apache.slider.providers.hbase.HBaseTestUtils
import org.apache.slider.server.appmaster.PublishedArtifacts
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.WatchedEvent
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZKUtil
import org.apache.zookeeper.ZooKeeper
import org.junit.After
import org.junit.Before
import org.junit.Test

import static org.apache.slider.providers.hbase.HBaseKeys.ROLE_MASTER
import static org.apache.slider.providers.hbase.HBaseKeys.ROLE_WORKER

@CompileStatic
@Slf4j
public class FunctionalHBaseClusterIT extends HBaseCommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes {


  public static final String HBASE_HEAP = "96m"

  public String getClusterName() {
    return "test_functional_hbase_cluster"
  }

  public String getClusterZNode() {
    return "/yarnapps_slider_yarn_" + clusterName;
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
      ZKUtil.deleteRecursive(monitor, clusterZNode)
    } catch (KeeperException.NoNodeException ignored) {
      log.info(clusterZNode + " not there")
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

    int numWorkers = desiredWorkerCount;

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
    ClusterDescription cd2 = sliderClient.clusterDescription
    assert clusterName == cd2.name

    log.info("Connected via Client {} with {} workers", sliderClient.toString(),
        numWorkers)

    //wait for the role counts to be reached
    waitForRoleCount(sliderClient, roleMap, HBASE_LAUNCH_WAIT_TIME)

    Configuration clientConf = HBaseTestUtils.createHBaseConfiguration(sliderClient)
    HBaseTestUtils.assertHBaseMasterFound(clientConf)
    HBaseTestUtils.waitForHBaseRegionServerCount(sliderClient,
        clusterName,
        numWorkers,
        HBASE_LAUNCH_WAIT_TIME)

    clusterOperations(
        clusterName,
        sliderClient,
        clientConf,
        numWorkers,
        roleMap,
        cd2)
  }

  /**
   * Override to change policy of the deired no of workers
   * @return
   */
  def int getDesiredWorkerCount() {
    return SLIDER_CONFIG.getInt(KEY_SLIDER_TEST_NUM_WORKERS,
        DEFAULT_SLIDER_NUM_WORKERS)
  }


  public String getDescription() {
    return "Create a working HBase cluster $clusterName"
  }

  /**
   * Override point for any cluster operations
   * @param clustername name of cluster
   * @param sliderClient bonded low level client
   * @param clientConf config
   * @param numWorkers no. of workers created
   * @param roleMap role map
   * @param cd current cluster
   */
  public void clusterOperations(
      String clustername,
      SliderClient sliderClient,
      Configuration clientConf,
      int numWorkers,
      Map<String, Integer> roleMap,
      ClusterDescription cd) {

    log.info("Client Configuration = " + ConfigHelper.dumpConfigToString(clientConf))
    
    //grab some registry bits
    registry([ARG_LIST])
    registry([ARG_LIST, ARG_SERVICETYPE, SliderKeys.APP_TYPE , ARG_VERBOSE])
    
    //unknown service type
    registry(EXIT_NOT_FOUND,
        [ARG_LIST, ARG_SERVICETYPE, "org.apache.something"])

    registry(EXIT_NOT_FOUND,
        [ARG_LIST, ARG_NAME, "cluster-with-no-name"])

    // how to work out the current service name?
    def name = RegistryNaming.createRegistryName(clustername,
        System.getProperty("user.name"),
        SliderKeys.APP_TYPE,
        1)
    registry([ARG_LIST, ARG_VERBOSE, ARG_NAME, name])
    
    registry([ARG_LISTCONF, ARG_NAME, name])
    registry(EXIT_NOT_FOUND, [ARG_LISTCONF, ARG_NAME, name, ARG_INTERNAL])
    registry(EXIT_NOT_FOUND, [ARG_LISTCONF, ARG_NAME, "unknown"])
    registry([ARG_GETCONF, PublishedArtifacts.COMPLETE_CONFIG,
              ARG_NAME, name])
    registry(EXIT_NOT_FOUND, [ARG_GETCONF, "no-such-config",
              ARG_NAME, name])

    registry(EXIT_NOT_FOUND, [ARG_GETCONF, "illegal/config/name!",
              ARG_NAME, name])

    registry(EXIT_NOT_FOUND,[ARG_GETCONF, PublishedArtifacts.COMPLETE_CONFIG,
              ARG_NAME, name, ARG_INTERNAL])


    def yarn_site_config = PublishedArtifacts.YARN_SITE_CONFIG
    registry([ARG_GETCONF, yarn_site_config,
              ARG_NAME, name])

    File getConfDir = new File("target/$clusterName/getconf")
    getConfDir.mkdirs();
    registry([ARG_GETCONF, yarn_site_config,
              ARG_NAME, name,
              ARG_DEST, getConfDir.absolutePath])
    File retrieved = new File(getConfDir, yarn_site_config +".xml")
    def confFromFile = ConfigHelper.loadConfFromFile(retrieved)
    assert confFromFile.get(YarnConfiguration.RM_ADDRESS)
    
  }

}
