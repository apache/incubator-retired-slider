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

package org.apache.slider.providers.hbase.minicluster.masterless

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.providers.hbase.HBaseKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Test

import static org.apache.slider.providers.hbase.HBaseKeys.PROVIDER_HBASE
import static org.apache.slider.common.params.Arguments.*

/**
 * create masterless AMs and work with them. This is faster than
 * bringing up full clusters
 */
@CompileStatic
@Slf4j

class TestKillMasterlessAM extends HBaseMiniClusterTestBase {


  @Test
  public void testKillMasterlessAM() throws Throwable {
    String clustername = "test_kill_masterless_am"
    createMiniCluster(clustername, getConfiguration(), 1, true)

    describe "kill a masterless AM and verify that it shuts down"

    Map<String, Integer> roles = [
        (HBaseKeys.ROLE_MASTER): 0,
        (HBaseKeys.ROLE_WORKER): 0,
    ]
    ServiceLauncher launcher = createCluster(clustername,
        roles,
        [
/*
            ARG_COMP_OPT, SliderKeys.COMPONENT_AM,
            RoleKeys.JVM_OPTS, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005",
*/
            ARG_PROVIDER, PROVIDER_HBASE
        ],
        true,
        true,
        [:])
    SliderClient sliderClient = (SliderClient) launcher.service
    addToTeardown(sliderClient);
    describe("listing services")
    lsJavaProcesses();
    describe("killing services")
    killServiceLaunchers(SIGTERM);
    waitWhileClusterLive(sliderClient);
    //give yarn some time to notice
    sleep(2000)
    describe("final listing")
    lsJavaProcesses();
    ApplicationReport report = sliderClient.applicationReport
    assert report.yarnApplicationState == YarnApplicationState.FAILED;



    clusterActionFreeze(sliderClient, clustername)
  }


}
