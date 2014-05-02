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

package org.apache.slider.providers.accumulo.live

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.api.ClusterDescription
import org.apache.slider.providers.accumulo.AccumuloKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.accumulo.AccumuloTestBase
import org.apache.slider.core.registry.zk.ZKIntegration
import org.junit.Test

@CompileStatic
@Slf4j
class TestAccM1T1GC1Mon1 extends AccumuloTestBase {

  @Test
  public void testAccM1T1GC1Mon1() throws Throwable {
    String clustername = "test_acc_m1t1gc1mon1"
    int tablets = 1
    int monitor = 1
    int gc = 1
    createMiniCluster(clustername, getConfiguration(), 1, 1, 1, true, false)
    describe(" Create an accumulo cluster");

    //make sure that ZK is up and running at the binding string
    ZKIntegration zki = createZKIntegrationInstance(ZKBinding, clustername, false, false, 5000)
    log.info("ZK up at $zki");
    //now launch the cluster
    Map<String, Integer> roles = [
        (AccumuloKeys.ROLE_MASTER): 1,
        (AccumuloKeys.ROLE_TABLET): tablets,
        (AccumuloKeys.ROLE_MONITOR): monitor,
        (AccumuloKeys.ROLE_GARBAGE_COLLECTOR): gc
    ];
    ServiceLauncher launcher = createAccCluster(clustername, roles, [], true, true)
    SliderClient sliderClient = launcher.service
    addToTeardown(sliderClient);


    waitWhileClusterLive(sliderClient);
    assert sliderClient.applicationReport.yarnApplicationState == YarnApplicationState.RUNNING
    waitForRoleCount(sliderClient, roles, ACCUMULO_CLUSTER_STARTUP_TO_LIVE_TIME)
    describe("Cluster status")
    ClusterDescription status
    status = sliderClient.getClusterDescription(clustername)
    log.info(prettyPrint(status.toJsonString()))

    //now give the cluster a bit of time to actually start work

    log.info("Sleeping for a while")
    sleep(ACCUMULO_GO_LIVE_TIME);

    //verify that all is still there
    waitForRoleCount(sliderClient, roles, 0)


    log.info("Finishing")
    status = sliderClient.getClusterDescription(clustername)
    log.info(prettyPrint(status.toJsonString()))
    maybeStopCluster(sliderClient, clustername, "shut down $clustername")

  }

}
