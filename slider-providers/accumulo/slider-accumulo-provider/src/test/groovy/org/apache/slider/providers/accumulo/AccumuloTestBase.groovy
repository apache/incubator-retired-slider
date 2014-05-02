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

package org.apache.slider.providers.accumulo

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.ResourceKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.test.YarnZKMiniClusterTestBase

import static org.apache.slider.common.SliderXMLConfKeysForTesting.*
import static org.apache.slider.providers.accumulo.AccumuloKeys.*
import static org.apache.slider.common.params.Arguments.ARG_PROVIDER
import static org.apache.slider.common.params.Arguments.ARG_RES_COMP_OPT

/**
 * test base for accumulo clusters
 */
@CompileStatic
@Slf4j
public abstract class AccumuloTestBase extends YarnZKMiniClusterTestBase {

  public static final int ACCUMULO_LAUNCH_WAIT_TIME
  public static final boolean ACCUMULO_TESTS_ENABLED


  public static final int ACCUMULO_CLUSTER_STARTUP_TIME = ACCUMULO_LAUNCH_WAIT_TIME
  public static final int ACCUMULO_CLUSTER_STOP_TIME = 1 * 60 * 1000

  /**
   * The time to sleep before trying to talk to the HBase Master and
   * expect meaningful results.
   */
  public static final int ACCUMULO_CLUSTER_STARTUP_TO_LIVE_TIME = ACCUMULO_CLUSTER_STARTUP_TIME
  public static final int ACCUMULO_GO_LIVE_TIME = 60000

  @Override
  public String getTestConfigurationPath() {
    return "src/main/resources/" + CONF_RESOURCE; 
  }

  @Override
  void setup() {
    super.setup()
    assumeBoolOption(SLIDER_CONFIG, KEY_TEST_ACCUMULO_ENABLED, true)
    assumeArchiveDefined();
    assumeApplicationHome();
    YarnConfiguration conf = testConfiguration
    assumeOtherSettings(conf)
  }

  /**
   * Teardown 
   */
  @Override
  void teardown() {
    super.teardown();
    if (teardownKillall) {
      killAllAccumuloProcesses();
    }
  }
  
  void killAllAccumuloProcesses() {
    killJavaProcesses("org.apache.accumulo.start.Main", SIGKILL)
  }

  @Override
  public String getArchiveKey() {
    return KEY_TEST_ACCUMULO_TAR
  }

  /**
   * Get the key for the application
   * @return
   */
  @Override
  public String getApplicationHomeKey() {
    return KEY_TEST_ACCUMULO_HOME
  }

  /**
   * Assume that HBase home is defined. This does not check that the
   * path is valid -that is expected to be a failure on tests that require
   * HBase home to be set.
   */
  
  public void assumeOtherSettings(YarnConfiguration conf) {
    assumeStringOptionSet(conf, OPTION_ZK_HOME)
  }

  /**
   * Create a full cluster with a master & the requested no. of region servers
   * @param clustername cluster name
   * @param tablets # of nodes
   * @param extraArgs list of extra args to add to the creation command
   * @param deleteExistingData should the data of any existing cluster
   * of this name be deleted
   * @param blockUntilRunning block until the AM is running
   * @return launcher which will have executed the command.
   */
  public ServiceLauncher<SliderClient> createAccCluster(String clustername, int tablets, List<String> extraArgs, boolean deleteExistingData, boolean blockUntilRunning) {
    Map<String, Integer> roles = [
        (ROLE_MASTER): 1,
        (ROLE_TABLET): tablets,
    ];
    return createAccCluster(clustername, roles, extraArgs, deleteExistingData, blockUntilRunning);
}

  /**
   * Create an accumulo cluster
   * @param clustername
   * @param roles
   * @param extraArgs
   * @param deleteExistingData
   * @param blockUntilRunning
   * @return the cluster launcher
   */
  public ServiceLauncher<SliderClient> createAccCluster(String clustername, Map<String, Integer> roles, List<String> extraArgs, boolean deleteExistingData, boolean blockUntilRunning) {
    extraArgs << ARG_PROVIDER << PROVIDER_ACCUMULO;

    YarnConfiguration conf = testConfiguration

    def clusterOps = [
        (OPTION_ZK_HOME): conf.getTrimmed(OPTION_ZK_HOME),
        (OPTION_HADOOP_HOME): conf.getTrimmed(OPTION_HADOOP_HOME),
        ("site." + AccumuloConfigFileOptions.MONITOR_PORT_CLIENT): AccumuloConfigFileOptions.MONITOR_PORT_CLIENT_DEFAULT,
        ("site." + AccumuloConfigFileOptions.MASTER_PORT_CLIENT): AccumuloConfigFileOptions.MASTER_PORT_CLIENT_DEFAULT,
    ]


    extraArgs << ARG_RES_COMP_OPT << ROLE_MASTER << ResourceKeys.YARN_MEMORY << YRAM; 
    extraArgs << ARG_RES_COMP_OPT << ROLE_TABLET << ResourceKeys.YARN_MEMORY << YRAM
    extraArgs << ARG_RES_COMP_OPT << ROLE_MONITOR << ResourceKeys.YARN_MEMORY << YRAM
    extraArgs << ARG_RES_COMP_OPT << ROLE_GARBAGE_COLLECTOR << ResourceKeys.YARN_MEMORY << YRAM

    return createCluster(clustername,
                             roles,
                             extraArgs,
                             deleteExistingData,
                             blockUntilRunning, 
                             clusterOps)
  }

  def getAccClusterStatus() {
    ZooKeeperInstance instance = new ZooKeeperInstance("", "localhost:4");
    instance.getConnector("user", "pass").instanceOperations().tabletServers;
  }

  
  public String fetchLocalPage(int port, String page) {
    String url = "http://localhost:" + port+ page
    return fetchWebPage(url)
    
  }

  public ClusterDescription flexAccClusterTestRun(
      String clustername, List<Map<String, Integer>> plan) {
    int planCount = plan.size()
    assert planCount > 0
    createMiniCluster(clustername, getConfiguration(),
                      1,
                      true);
    //now launch the cluster
    SliderClient sliderClient = null;
    ServiceLauncher launcher = createAccCluster(clustername,
                                                 plan[0],
                                                 [],
                                                 true,
                                                 true);
    sliderClient = (SliderClient) launcher.service;
    try {

      //verify the #of roles is as expected
      //get the hbase status
      waitForRoleCount(sliderClient, plan[0],
                       ACCUMULO_CLUSTER_STARTUP_TO_LIVE_TIME);
      sleep(ACCUMULO_GO_LIVE_TIME);

      plan.remove(0)

      ClusterDescription cd = null
      while (!plan.empty) {

        Map<String, Integer> flexTarget = plan.remove(0)
        //now flex
        describe(
            "Flexing " + roleMapToString(flexTarget));
        boolean flexed = 0 == sliderClient.flex(clustername,
            flexTarget
        );
        cd = waitForRoleCount(sliderClient, flexTarget,
                              ACCUMULO_CLUSTER_STARTUP_TO_LIVE_TIME);

        sleep(ACCUMULO_GO_LIVE_TIME);

      }
      
      return cd;

    } finally {
      maybeStopCluster(sliderClient, null, "end of flex test run");
    }

  }
  
}
