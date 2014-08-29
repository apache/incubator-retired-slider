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
import org.apache.slider.common.SliderKeys
import org.apache.slider.core.conf.MapOperations
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationResponse
import org.apache.slider.server.appmaster.web.rest.agent.RegistrationStatus
import org.apache.slider.server.services.security.CertificateManager
import org.apache.slider.server.services.security.SecurityUtils
import org.junit.Before
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.net.ssl.HostnameVerifier
import javax.net.ssl.HttpsURLConnection
import javax.net.ssl.SSLSession
import javax.ws.rs.core.MediaType

import static org.apache.slider.common.params.Arguments.ARG_OPTION
import static org.apache.slider.providers.agent.AgentKeys.*
import static org.apache.slider.providers.agent.AgentTestUtils.createDummyJSONRegister
import static org.apache.slider.providers.agent.AgentTestUtils.createTestClient

@CompileStatic
@Slf4j
class TestAgentAMManagementWS extends AgentTestBase {

  public static final String AGENT_URI = "ws/v1/slider/agents/";
    final static Logger logger = LoggerFactory.getLogger(TestAgentAMManagementWS.class)
    static {
        //for localhost testing only
        HttpsURLConnection.setDefaultHostnameVerifier(
                new HostnameVerifier(){
                    public boolean verify(String hostname,
                                          SSLSession sslSession) {
                        logger.info("verifying hostname ${hostname}")
                        InetAddress[] addresses =
                            InetAddress.getAllByName(hostname);
                        if (hostname.equals("localhost")) {
                            return true;
                        }
                        for (InetAddress address : addresses) {
                            if (address.getHostName().equals(hostname) ||
                                address.isAnyLocalAddress() ||
                                address.isLoopbackAddress()) {
                                return true;
                            }
                        }
                        return false;
                    }
                });

    }

    @Override
    @Before
    void setup() {
        super.setup()
        MapOperations compOperations = new MapOperations();
        compOperations.put(SliderKeys.KEYSTORE_LOCATION, "/tmp/work/security/keystore.p12");
        SecurityUtils.initializeSecurityParameters(compOperations);
        CertificateManager certificateManager = new CertificateManager();
        certificateManager.initRootCert(compOperations);
        String keystoreFile = SecurityUtils.getSecurityDir() + File.separator + SliderKeys.KEYSTORE_FILE_NAME;
        String password = SecurityUtils.getKeystorePass();
        System.setProperty("javax.net.ssl.trustStore", keystoreFile);
        System.setProperty("javax.net.ssl.trustStorePassword", password);
        System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");

    }

    @Test
  public void testAgentAMManagementWS() throws Throwable {
      String clustername = createMiniCluster("",
        configuration,
        1,
        1,
        1,
        true,
        false)
    Map<String, Integer> roles = [:]
    File slider_core = new File(new File(".").absoluteFile, "src/test/python");
    File app_def_path = new File(app_def_pkg_path)
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
            ARG_OPTION, APP_DEF, toURIArg(app_def_path),
            ARG_OPTION, AGENT_CONF, toURIArg(agt_conf_path),
            ARG_OPTION, AGENT_VERSION, toURIArg(agt_ver_path),
        ],
        true, true,
        true)
    SliderClient sliderClient = launcher.service
    def report = waitForClusterLive(sliderClient)
    def trackingUrl = report.trackingUrl
    log.info("tracking URL is $trackingUrl")
    def agent_url = trackingUrl + AGENT_URI

    
    def status = dumpClusterStatus(sliderClient, "agent AM")
    def liveURL = status.getInfo(StatusKeys.INFO_AM_AGENT_OPS_URL)
    if (liveURL) {
      agent_url = liveURL + AGENT_URI
    }
    
    log.info("Agent  is $agent_url")
    log.info("stacks is ${liveURL}stacks")
    log.info("conf   is ${liveURL}conf")


    def sleeptime = 10
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
