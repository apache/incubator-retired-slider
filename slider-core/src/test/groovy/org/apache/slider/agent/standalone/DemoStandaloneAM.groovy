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

package org.apache.slider.agent.standalone

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.core.main.ServiceLauncher
import org.junit.After
import org.junit.Test

/**
 * This is a test with the name "Demo" so it doesn't run.
 * All it does is start the AM, print the URLs and then
 * sleep for a while.
 */
@CompileStatic
@Slf4j
class DemoStandaloneAM extends AgentMiniClusterTestBase {



  @After
  void fixclientname() {
    sliderClientClassName = DEFAULT_SLIDER_CLIENT
  }
  
  @Test(timeout = 600000L)
  public void testDemoStandaloneAM() throws Throwable {

    describe "create a standalone AM then pause to allow the user " +
             "to explore the web and YARN UI integration"
    //launch fake master
    String clustername = createMiniCluster("", configuration, 1, true)


    ServiceLauncher<SliderClient> launcher =
        createStandaloneAM(clustername, true, false)
    SliderClient client = launcher.service
    addToTeardown(client);

    ApplicationReport report = waitForClusterLive(client)
    URI uri = new URI(report.originalTrackingUrl)

    logReport(report)

    describe "YARN RM proxy: ${report.trackingUrl}"
    describe "Direct Application ${report.originalTrackingUrl}"
    
    sleep(5 * 60 * 1000)
    
  }
}
