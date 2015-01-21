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

package org.apache.slider.server.appmaster.web.rest.publisher

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.ClientResponse
import com.sun.jersey.api.client.WebResource
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.util.Shell
import org.apache.slider.api.StatusKeys
import org.apache.slider.client.SliderClient
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.core.registry.docstore.PublishedConfiguration
import org.apache.slider.providers.agent.AgentTestBase
import org.apache.slider.server.appmaster.web.rest.RestPaths
import org.junit.Test

import javax.ws.rs.core.MediaType

import static org.apache.slider.common.params.Arguments.ARG_OPTION
import static org.apache.slider.common.params.Arguments.ARG_PROVIDER
import static org.apache.slider.providers.agent.AgentKeys.*
import static org.apache.slider.providers.agent.AgentTestUtils.createTestClient

@CompileStatic
@Slf4j
class TestPublisherRestResources extends AgentTestBase {

  public static final String PUBLISHER_URI = RestPaths.SLIDER_PATH_PUBLISHER;
  public static final String WADL = "vnd.sun.wadl+xml"

  @Test
  public void testRestURIs() throws Throwable {
    String clustername = createMiniCluster(
        "",
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
            ARG_PROVIDER, "org.apache.slider.server.appmaster.web.rest.publisher.TestSliderProviderFactory",
            ARG_OPTION, PACKAGE_PATH, slider_core.absolutePath,
            ARG_OPTION, APP_DEF, toURIArg(app_def_path),
            ARG_OPTION, AGENT_CONF, toURIArg(agt_conf_path),
            ARG_OPTION, AGENT_VERSION, toURIArg(agt_ver_path)
        ],
        true, true,
        true)
    SliderClient sliderClient = launcher.service
    def report = waitForClusterLive(sliderClient)
    def trackingUrl = report.trackingUrl
    log.info("tracking URL is $trackingUrl")
    def publisher_url = appendToURL(trackingUrl, PUBLISHER_URI)

    
    def status = dumpClusterStatus(sliderClient, "agent AM")
    def liveURL = status.getInfo(StatusKeys.INFO_AM_WEB_URL) 
    if (liveURL) {
      publisher_url = appendToURL(liveURL, PUBLISHER_URI)
      
    }
    
    log.info("Publisher  is $publisher_url")
    log.info("stacks is ${liveURL}stacks")
    log.info("conf   is ${liveURL}conf")


    //WS get
    Client client = createTestClient();

    // test the available GET URIs
    String sliderConfigset = publisher_url +"/"+ RestPaths.SLIDER_CONFIGSET + "/"
    WebResource webResource
    webResource = client.resource(sliderConfigset);
    webResource = client.resource(sliderConfigset + "dummy-site");


    execOperation(30000) {
      GET(sliderConfigset)
    }

    PublishedConfiguration config = webResource.type(MediaType.APPLICATION_JSON)
                          .get(PublishedConfiguration.class);
    assert config != null
    Map<String,String> entries = config.entries
    log.info("entries are {}", entries)
    assert entries.get("prop1") =="val1"
    assert entries.get("prop2")== "val2"

    webResource = client.resource(sliderConfigset + "dummy-site/prop1");
    Map<String,String> val = webResource.type(MediaType.APPLICATION_JSON).get(Map.class);
    assert "val1".equals(val.get("prop1"))

    // testing security_enabled auto-setting feature (SLIDER-392)
    webResource = client.resource(sliderConfigset +
                                  "global/site.global.security_enabled");
    val = webResource.type(MediaType.APPLICATION_JSON).get(Map.class);
    assert "false".equals(val.get("site.global.security_enabled"))

      // some negative tests...
    webResource = client.resource(appendToURL(sliderConfigset,
        "foobar-site"));

    ClientResponse response = webResource.type(MediaType.APPLICATION_JSON)
                         .get(ClientResponse.class);
    assert 404 == response.status

    webResource = client.resource(sliderConfigset + "dummy-site/missing.prop");
    response = webResource.type(MediaType.TEXT_PLAIN).get(ClientResponse.class);
    assert 404 == response.status

    String classpathUri = publisher_url +"/"+ RestPaths.SLIDER_CLASSPATH
    webResource = client.resource(classpathUri)
    Set uris = webResource.type(MediaType.APPLICATION_JSON)
            .get(Set.class)
    assert uris.size() > 0
    if (!Shell.WINDOWS) {
      log.info("Classpath URIs: {}", uris)
      // check for some expected classpath elements
      assert uris.any {it =~ /hadoop-yarn-api/}
      assert uris.any {it =~ /hadoop-hdfs/}
      // and a negative test...
      assert !uris.any {it =~ /foo-bar/}
    }
  }

}
