/**
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

package org.apache.slider.server.appmaster.web.rest.agent;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import junit.framework.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.server.appmaster.model.mock.MockFactory;
import org.apache.slider.server.appmaster.model.mock.MockProviderService;
import org.apache.slider.server.appmaster.model.mock.MockRecordFactory;
import org.apache.slider.server.appmaster.model.mock.MockSliderClusterProtocol;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.WebAppApiImpl;
import org.apache.slider.server.appmaster.web.rest.AMWebServices;
import org.apache.slider.server.appmaster.web.rest.SliderJacksonJaxbJsonProvider;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class TestAMAgentWebServices extends JerseyTest {
  protected static final Logger log =
    LoggerFactory.getLogger(TestAMAgentWebServices.class);
  
  public static final int RM_MAX_RAM = 4096;
  public static final int RM_MAX_CORES = 64;
  public static final String AGENT_URL =
    "http://localhost:9998/slideram/ws/v1/slider/agents/";
  
  static MockFactory factory = new MockFactory();
  private static Configuration conf = new Configuration();
  private static WebAppApi slider;

  private static Injector injector = createInjector();
  private static FileSystem fs;

  public static class GuiceServletConfig extends GuiceServletContextListener {

    public GuiceServletConfig() {
      super();
    }

    @Override
    protected Injector getInjector() {
      return injector;
    }
  }

//  @Path("/ws/v1/slider/agent")
  @Path("/ws/v1/slider")
  public static class MockAMWebServices extends AMWebServices {

    @Inject
    public MockAMWebServices(WebAppApi slider) {
      super(slider);
    }

  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    injector = createInjector();
    YarnConfiguration conf = SliderUtils.createConfiguration();
    fs = FileSystem.get(new URI("file:///"), conf);
  }

  private static Injector createInjector() {
    return Guice.createInjector(new ServletModule() {
      @Override
      protected void configureServlets() {

        AppState appState = null;
        try {
          fs = FileSystem.get(new URI("file:///"), conf);
          File
              historyWorkDir =
              new File("target/history", "TestAMAgentWebServices");
          org.apache.hadoop.fs.Path
              historyPath =
              new org.apache.hadoop.fs.Path(historyWorkDir.toURI());
          fs.delete(historyPath, true);
          appState = new AppState(new MockRecordFactory());
          appState.setContainerLimits(RM_MAX_RAM, RM_MAX_CORES);
          appState.buildInstance(
              factory.newInstanceDefinition(0, 0, 0),
              new Configuration(false),
              factory.ROLES,
              fs,
              historyPath,
              null, null);
        } catch (Exception e) {
          log.error("Failed to set up app {}", e);
        }
        slider = new WebAppApiImpl(new MockSliderClusterProtocol(), appState,
                                   new MockProviderService());

        bind(SliderJacksonJaxbJsonProvider.class);
        bind(GenericExceptionHandler.class);
        bind(MockAMWebServices.class);
        bind(WebAppApi.class).toInstance(slider);
        bind(Configuration.class).toInstance(conf);

        Map<String, String> params = new HashMap<String, String>();
        addLoggingFilter(params);
        serve("/*").with(GuiceContainer.class, params);
      }
    });
  }

  private static void addLoggingFilter(Map<String, String> params) {
    params.put("com.sun.jersey.spi.container.ContainerRequestFilters", "com.sun.jersey.api.container.filter.LoggingFilter");
    params.put("com.sun.jersey.spi.container.ContainerResponseFilters", "com.sun.jersey.api.container.filter.LoggingFilter");
  }

  public TestAMAgentWebServices() {
    super(new WebAppDescriptor.Builder(
      "org.apache.hadoop.yarn.appmaster.web")
            .contextListenerClass(GuiceServletConfig.class)
            .filterClass(com.google.inject.servlet.GuiceFilter.class)
            .initParam("com.sun.jersey.api.json.POJOMappingFeature", "true")
            .contextPath("slideram").servletPath("/").build());
  }

  @Test
  public void testRegistration() throws JSONException, Exception {
    RegistrationResponse response;
    Client client = createTestClient();
    WebResource webResource = client.resource(AGENT_URL + "test/register");
    response = webResource.type(MediaType.APPLICATION_JSON)
        .post(RegistrationResponse.class, createDummyJSONRegister());
    Assert.assertEquals(RegistrationStatus.OK, response.getResponseStatus());
  }

  protected Client createTestClient() {
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put(JSONConfiguration.FEATURE_POJO_MAPPING, Boolean.TRUE);
    return Client.create(clientConfig);
  }

  @Test
  public void testHeartbeat() throws JSONException, Exception {
    HeartBeatResponse response;
    Client client = createTestClient();
    WebResource webResource = client.resource(AGENT_URL + "test/heartbeat");
    response = webResource.type(MediaType.APPLICATION_JSON)
        .post(HeartBeatResponse.class, createDummyHeartBeat());
    assertEquals(response.getResponseId(), 0L);
  }

  @Test
  public void testHeadURL() throws JSONException, Exception {
    Client client = createTestClient();
    WebResource webResource = client.resource(AGENT_URL);
    ClientResponse response = webResource.type(MediaType.APPLICATION_JSON)
                                         .head();
    assertEquals(200, response.getStatus());
  }

  @Test
  public void testSleepForAWhile() throws Throwable {
    log.info("Agent is running at {}", AGENT_URL);
    Thread.sleep(60 * 1000);
  }
  
  private Register createDummyJSONRegister() throws JSONException {
    Register register = new Register();
    register.setResponseId(-1);
    register.setTimestamp(System.currentTimeMillis());
    register.setHostname("dummyHost");
    return register;
  }

  private JSONObject createDummyHeartBeat() throws JSONException {
    JSONObject json = new JSONObject();
    json.put("responseId", -1);
    json.put("timestamp", System.currentTimeMillis());
    json.put("hostname", "dummyHost");
    return json;
  }

}
