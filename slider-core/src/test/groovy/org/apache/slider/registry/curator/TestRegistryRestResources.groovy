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

package org.apache.slider.registry.curator

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.UniformInterfaceException
import com.sun.jersey.api.client.WebResource
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.curator.x.discovery.ServiceType
import org.apache.hadoop.security.UserGroupInformation
import org.apache.slider.api.StatusKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderKeys
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.registry.info.ServiceInstanceData
import org.apache.slider.providers.agent.AgentTestBase
import org.apache.slider.server.appmaster.web.rest.RestPaths
import org.apache.slider.server.services.curator.CuratorServiceInstance
import org.apache.slider.server.services.curator.CuratorServiceInstances
import org.apache.slider.server.services.curator.RegistryNaming
import org.junit.Test

import javax.ws.rs.core.MediaType

import static org.apache.slider.common.params.Arguments.ARG_OPTION
import static org.apache.slider.providers.agent.AgentKeys.*
import static org.apache.slider.providers.agent.AgentTestUtils.createTestClient

@CompileStatic
@Slf4j
class TestRegistryRestResources extends AgentTestBase {

  public static final String REGISTRY_URI = RestPaths.SLIDER_PATH_REGISTRY;

  
  private String id(String instanceName) {

    RegistryNaming.createUniqueInstanceId(
        instanceName,
        UserGroupInformation.getCurrentUser().getUserName(),
        SliderKeys.APP_TYPE,
        1);
  }


  @Test
  public void testRestURIs() throws Throwable {
    def clustername = "test_registryws"
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

    // test the exposed WADL link
    WebResource webResource = client.resource(registry_url);
    ClientResponse response = webResource.type(MediaType.APPLICATION_XML)
           .get(ClientResponse.class);
    assert response.status == 200
    assert response.getType() == (new MediaType("application", "vnd.sun.wadl+xml"))

    // test the available GET URIs
    webResource = client.resource(
        appendToURL(registry_url, RestPaths.REGISTRY_SERVICE));
    
    response = webResource.type(MediaType.APPLICATION_JSON)
                          .get(ClientResponse.class);
    def responseStr = response.getEntity(String.class)
    log.info("response is " + responseStr)

     "{\"names\":[\"${SliderKeys.APP_TYPE}\"]}".equals(responseStr)

    webResource = client.resource(
        appendToURL(registry_url,
            "${RestPaths.REGISTRY_SERVICE}/${SliderKeys.APP_TYPE}"));
    CuratorServiceInstances<ServiceInstanceData> services = webResource.type(MediaType.APPLICATION_JSON)
            .get(CuratorServiceInstances.class);
    assert services.services.size() == 1
    CuratorServiceInstance<ServiceInstanceData> service = services.services.get(0)
    validateService(service)

    webResource = client.resource(
        appendToURL(registry_url,
            "${RestPaths.REGISTRY_SERVICE}/${SliderKeys.APP_TYPE}/"+id("test_registryws")));
    service = webResource.type(MediaType.APPLICATION_JSON)
              .get(CuratorServiceInstance.class);
    validateService(service)

    webResource = client.resource(
        appendToURL(
            registry_url, "${RestPaths.REGISTRY_ANYSERVICE}/${SliderKeys.APP_TYPE}"));
    service = webResource.type(MediaType.APPLICATION_JSON)
            .get(CuratorServiceInstance.class);
    validateService(service)

    // some negative tests...
    webResource = client.resource(
        appendToURL(registry_url, "${RestPaths.REGISTRY_SERVICE}/dummy"))
    services = webResource.type(MediaType.APPLICATION_JSON)
            .get(CuratorServiceInstances.class);
    assert services.services.size() == 0

    try {
      webResource = client.resource(appendToURL(registry_url,
          "${RestPaths.REGISTRY_SERVICE}/${SliderKeys.APP_TYPE}/test_registryws-99"));
      
      service = webResource.type(MediaType.APPLICATION_JSON)
                           .get(CuratorServiceInstance.class);
      fail("should throw an exception for a 404 response....")
    } catch (UniformInterfaceException e) {
        assert e.response.getStatus() == 404
    }

    try {
      webResource = client.resource(
          appendToURL(registry_url, "${RestPaths.REGISTRY_ANYSERVICE}/dummy"));
      
      service = webResource.type(MediaType.APPLICATION_JSON)
                           .get(CuratorServiceInstance.class);
      fail("should throw an exception for a 404 response....")
    } catch (UniformInterfaceException e) {
        assert e.response.getStatus() == 404
    }
 }

  private void validateService(CuratorServiceInstance service) {
    assert service.name.equals(SliderKeys.APP_TYPE)
    assert service.serviceType == ServiceType.DYNAMIC
    assert service.id.contains("test_registryws")
  }
}
