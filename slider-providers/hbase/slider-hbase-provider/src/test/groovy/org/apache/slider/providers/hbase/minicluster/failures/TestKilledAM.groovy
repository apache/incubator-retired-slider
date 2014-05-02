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

package org.apache.slider.providers.hbase.minicluster.failures

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.ClusterStatus
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fifo.FifoScheduler
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.StatusKeys
import org.apache.slider.providers.hbase.HBaseKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.ActionAMSuicideArgs
import org.apache.slider.providers.hbase.minicluster.HBaseMiniClusterTestBase
import org.junit.Test

/**
 * test create a live region service
 */
@CompileStatic
@Slf4j
class TestKilledAM extends HBaseMiniClusterTestBase {

  @Test
  public void testKilledAM() throws Throwable {
    skip("failing")
    
    String clustername = "test_killed_am"
    int regionServerCount = 1


    def conf = configuration
    // patch the configuration for AM restart
    conf.setInt(SliderXmlConfKeys.KEY_AM_RESTART_LIMIT, 3)

    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        FifoScheduler, ResourceScheduler);
    createMiniCluster(clustername, conf, 1, 1, 1, true, false)
    describe(" Kill the AM, expect cluster to die");

    //now launch the cluster
    ServiceLauncher<SliderClient> launcher = createHBaseCluster(
        clustername,
        regionServerCount,
        [],
        true,
        true)
    SliderClient sliderClient = launcher.service
    addToTeardown(sliderClient);
    ClusterDescription status = sliderClient.getClusterDescription(clustername)

    ClusterStatus clustat = basicHBaseClusterStartupSequence(sliderClient)


    status = waitForWorkerInstanceCount(
        sliderClient,
        regionServerCount,
        hbaseClusterStartupToLiveTime)
    //get the hbase status
    ClusterStatus hbaseStat = waitForHBaseRegionServerCount(
        sliderClient,
        clustername,
        regionServerCount,
        hbaseClusterStartupToLiveTime)

    log.info("Initial cluster status : ${hbaseStatusToString(hbaseStat)}");

    String hbaseMasterContainer = status.instances[HBaseKeys.ROLE_MASTER][0]

    Configuration clientConf = createHBaseConfiguration(sliderClient)
    clientConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    HConnection hbaseConnection
    hbaseConnection = createHConnection(clientConf)



    describe("running processes")
    lsJavaProcesses()
    describe("killing AM")

    ActionAMSuicideArgs args = new ActionAMSuicideArgs()
    args.message = "test AM"
    args.waittime = 1000
    args.exitcode = 1
    sliderClient.actionAmSuicide(clustername, args)

    killAllRegionServers();
    waitWhileClusterLive(sliderClient);
    // give yarn some time to notice
    sleep(10000)

    // await cluster startup
    ApplicationReport report = sliderClient.applicationReport
    assert report.yarnApplicationState != YarnApplicationState.FAILED;


    def restartTime = 60000
    status = waitForWorkerInstanceCount(
        sliderClient,
        regionServerCount,
        restartTime)

    dumpClusterDescription("post-restart status", status)
    String restarted = status.getInfo(
        StatusKeys.INFO_CONTAINERS_AM_RESTART)
    assert restarted != null
    assert Integer.parseInt(restarted) == 1
    assert null != status.instances[HBaseKeys.ROLE_MASTER]
    assert 1 == status.instances[HBaseKeys.ROLE_MASTER].size()
    assert hbaseMasterContainer == status.instances[HBaseKeys.ROLE_MASTER][0]

    waitForHBaseRegionServerCount(
        sliderClient,
        clustername,
        regionServerCount,
        restartTime)

  }

}
