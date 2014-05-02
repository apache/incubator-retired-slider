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

import org.apache.hadoop.conf.Configuration
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.RoleKeys
import org.apache.slider.api.StatusKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.ActionKillContainerArgs
import org.apache.slider.providers.hbase.HBaseKeys
import org.apache.slider.providers.hbase.HBaseTestUtils

class TestHBaseNodeFailure extends TestFunctionalHBaseCluster {


  public static final int RESTART_SLEEP_TIME = 5000

  @Override
  String getClusterName() {
    return "test_hbase_node_failure"
  }

  @Override
  String getDescription() {
    "Fail containers and verify that the cluster recovers"
  }

  @Override
  void clusterLoadOperations(
      String clustername,
      Configuration clientConf,
      int numWorkers,
      Map<String, Integer> roleMap,
      ClusterDescription cd) {
    SliderClient sliderClient = bondToCluster(SLIDER_CONFIG, clusterName)


    killInstanceOfRole(sliderClient, HBaseKeys.ROLE_WORKER)
    // let it take
    sleep(RESTART_SLEEP_TIME)

    //wait for the role counts to be reached
    cd = waitForRoleCount(sliderClient, roleMap, HBASE_LAUNCH_WAIT_TIME)
    // then expect a restart
    HBaseTestUtils.waitForHBaseRegionServerCount(
        sliderClient,
        clusterName,
        numWorkers,
        HBASE_LAUNCH_WAIT_TIME)
    assert cd.roles[HBaseKeys.ROLE_WORKER][RoleKeys.ROLE_FAILED_INSTANCES] == "1"
    killInstanceOfRole(sliderClient, HBaseKeys.ROLE_WORKER)
    // let it take
    sleep(RESTART_SLEEP_TIME)
    // then expect a restart

    //wait for the role counts to be reached
    cd = waitForRoleCount(sliderClient, roleMap, HBASE_LAUNCH_WAIT_TIME)

    HBaseTestUtils.waitForHBaseRegionServerCount(
        sliderClient,
        clusterName,
        numWorkers,
        HBASE_LAUNCH_WAIT_TIME)
    assert cd.roles[HBaseKeys.ROLE_WORKER][RoleKeys.ROLE_FAILED_INSTANCES] == "2"

    killInstanceOfRole(sliderClient, HBaseKeys.ROLE_MASTER)
    // let it take
    sleep(RESTART_SLEEP_TIME)
    
    // wait for the role counts to be reached
    cd = waitForRoleCount(sliderClient, roleMap, HBASE_LAUNCH_WAIT_TIME)
    HBaseTestUtils.waitForHBaseRegionServerCount(
        sliderClient,
        clusterName,
        numWorkers,
        HBASE_LAUNCH_WAIT_TIME)
    assert cd.roles[HBaseKeys.ROLE_MASTER][RoleKeys.ROLE_FAILED_INSTANCES] == "1"

    // now trigger AM failure
    ClusterDescription status = killAmAndWaitForRestart(sliderClient, clusterName)

    def restarted = status.getInfo(
        StatusKeys.INFO_CONTAINERS_AM_RESTART)
    assert restarted != null
    assert Integer.parseInt(restarted) == 1 + numWorkers

  }

  /**
   * Kill a random in instance of a role in the cluster
   * @param sliderClient client
   * @param role
   * @return ID of container killed
   */
  public String killInstanceOfRole(
      SliderClient sliderClient, String role) {
    ClusterDescription cd = sliderClient.getClusterDescription()
    def instances = cd.instances[role]
    if (instances == null || instances.size() == 0) {
      log.info("No instances of role $role to kill")
      return null;
    }
    String id = instances[new Random().nextInt(instances.size())]
    ActionKillContainerArgs args = new ActionKillContainerArgs()
    args.id = id
    sliderClient.actionKillContainer(clusterName, args)
    return id;
  }


  public int getWorkerPortAssignment() {
    return 0
  }

  public int getMasterPortAssignment() {
    return 0
  }
}
