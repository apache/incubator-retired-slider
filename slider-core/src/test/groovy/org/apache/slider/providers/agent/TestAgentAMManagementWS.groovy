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

package org.apache.slider.providers.agent

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.WebResource
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.api.StatusKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.server.appmaster.web.SliderAMWebApp
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationResponse
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationStatus
import org.junit.Test

import javax.ws.rs.core.MediaType

import static org.apache.slider.common.params.Arguments.ARG_OPTION
import static org.apache.slider.providers.agent.AgentKeys.*
import static org.apache.slider.providers.agent.AgentTestUtils.createDummyJSONRegister
import static org.apache.slider.providers.agent.AgentTestUtils.createTestClient
import static org.apache.slider.test.SliderTestUtils.log

@CompileStatic
@Slf4j
class TestAgentAMManagementWS extends AgentTestBase {

  public static final String MANAGEMENT_URI = SliderAMWebApp.BASE_PATH +"/ws/v1/slider/mgmt/";
  public static final String AGENT_URI = "ws/v1/slider/agents/";

  @Test
  public void testAgentAMManagementWS() throws Throwable {
    def clustername = "test_agentammanagementws"
    createMiniCluster(
        clustername,
        configuration,
        1,
        1,
        1,
        true,
        false)
    Map<String, Integer> roles = [:]
    File slider_core = new File(new File(".").absoluteFile, "src/test/python");
    String app_def = "appdef_1.tar"
    File app_def_path = new File(slider_core, app_def)
    String agt_ver = "version"
    File agt_ver_path = new File(slider_core, agt_ver)
    String agt_conf = "agent.ini"
    File agt_conf_path = new File(slider_core, agt_conf)
    assert app_def_path.exists()
    assert agt_ver_path.exists()
    assert agt_conf_path.exists()
    ServiceLauncher<SliderClient> launcher = buildAgentCluster(clustername,
        roles,
        [
            ARG_OPTION, PACKAGE_PATH, slider_core.absolutePath,
            ARG_OPTION, APP_DEF, "file://" + app_def_path.absolutePath,
            ARG_OPTION, AGENT_CONF, "file://" + agt_conf_path.absolutePath,
            ARG_OPTION, AGENT_VERSION, "file://" + agt_ver_path.absolutePath,
        ],
        true, true,
        true)
    SliderClient sliderClient = launcher.service
    def report = waitForClusterLive(sliderClient)
    def trackingUrl = report.trackingUrl
    log.info("tracking URL is $trackingUrl")
    def agent_url = trackingUrl + AGENT_URI

    
    def status = dumpClusterStatus(sliderClient, "agent AM")
    def liveURL = status.getInfo(StatusKeys.INFO_AM_WEB_URL) 
    if (liveURL) {
      agent_url = liveURL + AGENT_URI
    }
    
    log.info("Agent  is $agent_url")
    log.info("stacks is ${liveURL}stacks")
    log.info("conf   is ${liveURL}conf")


    def sleeptime = 60
    log.info "sleeping for $sleeptime seconds"
    Thread.sleep(sleeptime * 1000)
    

    String page = fetchWebPageWithoutError(agent_url);
    log.info(page);
    
    //WS get
    Client client = createTestClient();


    WebResource webResource = client.resource(agent_url + "test/register");
    RegistrationResponse response = webResource.type(MediaType.APPLICATION_JSON)
                          .post(
        RegistrationResponse.class,
        createDummyJSONRegister());

    //TODO: assert failure as actual agent is not started. This test only starts the AM.
    assert RegistrationStatus.FAILED == response.getResponseStatus();
    
  }
}
