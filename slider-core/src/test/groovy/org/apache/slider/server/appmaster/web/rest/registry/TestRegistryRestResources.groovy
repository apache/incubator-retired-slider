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

package org.apache.slider.server.appmaster.web.rest.registry

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.UniformInterfaceException
import com.sun.jersey.api.client.WebResource
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.registry.client.api.RegistryConstants
import org.apache.hadoop.registry.client.binding.RegistryUtils
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes
import org.apache.slider.api.StatusKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderKeys
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.providers.agent.AgentTestBase
import org.apache.slider.server.appmaster.web.rest.RestPaths
import org.junit.Ignore
import org.junit.Test

import javax.ws.rs.core.MediaType

import static org.apache.slider.common.params.Arguments.ARG_OPTION
import static org.apache.slider.core.registry.info.CustomRegistryConstants.*
import static org.apache.slider.providers.agent.AgentKeys.*
import static org.apache.slider.providers.agent.AgentTestUtils.createTestClient

@CompileStatic
@Slf4j
class TestRegistryRestResources extends AgentTestBase {

  public static final String REGISTRY_URI = RestPaths.SLIDER_PATH_REGISTRY;
  public static final String WADL = "vnd.sun.wadl+xml"
  public static final String CLUSTERNAME = "test-registry-rest-resources"


  @Test
  @Ignore("SLIDER-531")
  public void testRestURIs() throws Throwable {

    def clustername = CLUSTERNAME
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
    def registry_url = appendToURL(trackingUrl, REGISTRY_URI)


    def status = dumpClusterStatus(sliderClient, "agent AM")
    def liveURL = status.getInfo(StatusKeys.INFO_AM_WEB_URL)
    if (liveURL) {
      registry_url = appendToURL(liveURL, REGISTRY_URI)

    }

    log.info("Registry  is $registry_url")
    log.info("stacks is ${liveURL}stacks")
    log.info("conf   is ${liveURL}conf")

    //WS get
    Client client = createTestClient();

    WebResource webResource = client.resource(registry_url);

    def jsonType = webResource.type(MediaType.APPLICATION_JSON)
    PathEntryResource entryResource = jsonType
        .get(PathEntryResource.class);
    processResponse("/", jsonType.get(ClientResponse.class))
    assert entryResource.nodes != null;
    assert entryResource.service == null;

    // test the available GET URIs
    def userhome = RegistryUtils.homePathForCurrentUser()

    def userServicesURL = appendToURL(registry_url,
        userhome + RegistryConstants.PATH_USER_SERVICES)
    webResource = client.resource(userServicesURL);


    def jsonBuilder = webResource.type(MediaType.APPLICATION_JSON)
    ClientResponse response = jsonBuilder.get(ClientResponse.class);
    def responseStr = processResponse(userServicesURL, response)

    assert responseStr.contains("\"nodes\"")
    assert responseStr.contains(SliderKeys.APP_TYPE)

    entryResource = jsonBuilder.get(PathEntryResource.class)
    assert entryResource.nodes.size() == 1;
    assert entryResource.service == null;


    def userServicesSlider = appendToURL(userServicesURL, SliderKeys.APP_TYPE)
    webResource = client.resource(
        userServicesSlider);
    jsonBuilder = webResource.type(MediaType.APPLICATION_JSON);
    response = jsonBuilder.get(ClientResponse.class);
    processResponse(userServicesURL, response)
    entryResource = jsonBuilder.get(PathEntryResource.class)
    assert entryResource.nodes.size() == 1;
    assert entryResource.service == null;

    def servicePath = entryResource.nodes[0]

    // now work with a real instances
    
    def instanceURL = appendToURL(userServicesSlider, clustername)
    assert instanceURL.endsWith(servicePath)

    webResource = client.resource(instanceURL);

    // work with it via direct Jackson unmarshalling
    responseStr = processResponse(instanceURL, webResource)
    PathEntryMarshalling pem = new PathEntryMarshalling();
    def unmarshalled = pem.fromJson(responseStr)
    def r1 = unmarshalled.service
    assert r1
    assert r1[YarnRegistryAttributes.YARN_ID] != null
    assert r1[YarnRegistryAttributes.YARN_PERSISTENCE] != ""


    // and via the web resource AP
    jsonBuilder = webResource.type(MediaType.APPLICATION_JSON);
    entryResource = jsonBuilder.get(PathEntryResource.class);

    def serviceRecord = entryResource.service
    assert serviceRecord != null;
    assert serviceRecord[YarnRegistryAttributes.YARN_ID] != null
    def externalEndpoints = serviceRecord.external;
    assert externalEndpoints.size() > 0

    def am_ipc_protocol = AM_IPC_PROTOCOL
    def epr = serviceRecord.getExternalEndpoint(am_ipc_protocol)
    assert null != epr;

    assert null != serviceRecord.getExternalEndpoint(MANAGEMENT_REST_API)
    assert null != serviceRecord.getExternalEndpoint(PUBLISHER_REST_API)
    // internals
    assert null != serviceRecord.getInternalEndpoint(AGENT_ONEWAY_REST_API)
    assert null != serviceRecord.getInternalEndpoint(AGENT_SECURE_REST_API)

    // negative tests...
    try {
      webResource = client.resource(
          appendToURL(registry_url, "/users/no-such-user"));
      def clientResponse = webResource.get(ClientResponse.class)
      assert 404 == clientResponse.status
      
      def body = processResponse(userServicesURL, webResource)
      jsonBuilder = webResource.type(MediaType.APPLICATION_JSON);
      entryResource = jsonBuilder.get(PathEntryResource.class);


      fail("should throw an exception for a 404 response, got " + body)
    } catch (UniformInterfaceException e) {
      assert e.response.status == 404
    }
  }

  public String processResponse(String asURL, WebResource response) {
    def jsonBuilder = response.type(MediaType.APPLICATION_JSON)
    def clientResponse = jsonBuilder.get(ClientResponse.class);
    return processResponse(asURL, clientResponse)
  }
  
  public String processResponse(String asURL, ClientResponse response) {
    def responseStr = response.getEntity(String.class)
    log.info(asURL + " ==>\n " + responseStr)
    responseStr
  }

}
