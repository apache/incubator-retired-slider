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
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.api.ClusterDescription
import org.apache.slider.providers.accumulo.AccumuloConfigFileOptions
import org.apache.slider.providers.accumulo.AccumuloKeys
import org.apache.slider.server.appmaster.web.SliderAMWebApp
import org.apache.slider.client.SliderClient
import org.apache.slider.providers.accumulo.AccumuloTestBase
import org.apache.slider.core.registry.zk.ZKIntegration
import org.junit.Test

@CompileStatic
@Slf4j
class TestAccumuloAMWebApp extends AccumuloTestBase {

  @Test
  public void testAccumuloAMWebApp() throws Throwable {
    String clustername = "test_accumulo_am_webapp"
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
    SliderClient sliderClient = (SliderClient) launcher.service
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

    String page = fetchLocalPage(AccumuloConfigFileOptions.MONITOR_PORT_CLIENT_INT,
                                 AccumuloKeys.MONITOR_PAGE_JSON)
    log.info(page);

    log.info("Finishing")
    
    ApplicationReport appReport = sliderClient.getApplicationReport();
    
    String url = appReport.getTrackingUrl();
    
    // Should redirect to (or at least serve content from) SliderAMWebApp.BASE_PATH 
    fetchWebPageWithoutError(url);
    
    // TrackingUrl has a trailing slash on it already for us (which is apparently very important)
    // let's make sure it's there.
    if ('/' != url.charAt(url.length() -1)) {
      url = url + '/';
    }
    
    url = url + SliderAMWebApp.BASE_PATH;
    
    // This should also give us content
    fetchWebPageWithoutError(url);
    
    url = url + SliderAMWebApp.CONTAINER_STATS;
    
    fetchWebPageWithoutError(url);
    
    
    status = sliderClient.getClusterDescription(clustername)
    log.info(prettyPrint(status.toJsonString()))
    maybeStopCluster(sliderClient, clustername, "shut down $clustername")

  }

}
