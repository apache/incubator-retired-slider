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

package org.apache.slider.server.appmaster.web.rest.management;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.test.framework.JerseyTest;
import com.sun.jersey.test.framework.WebAppDescriptor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.persist.JsonSerDeser;
import org.apache.slider.server.appmaster.model.mock.MockAppState;
import org.apache.slider.server.appmaster.model.mock.MockClusterServices;
import org.apache.slider.server.appmaster.model.mock.MockFactory;
import org.apache.slider.server.appmaster.model.mock.MockProviderService;
import org.apache.slider.server.appmaster.state.AppState;
import org.apache.slider.server.appmaster.state.AppStateBindingInfo;
import org.apache.slider.server.appmaster.state.ProviderAppState;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.WebAppApiImpl;
import org.apache.slider.server.appmaster.web.rest.AMWebServices;
import org.apache.slider.server.appmaster.web.rest.SliderJacksonJaxbJsonProvider;
import org.apache.slider.server.appmaster.web.rest.management.resources.AggregateConfResource;
import org.apache.slider.server.appmaster.web.rest.management.resources.ConfTreeResource;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestAMManagementWebServices extends JerseyTest {
  protected static final Logger log =
      LoggerFactory.getLogger(TestAMManagementWebServices.class);
  public static final String EXAMPLES = "/org/apache/slider/core/conf/examples/";
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

  @Path("/ws/v1/slider")
  public static class MockSliderAMWebServices extends AMWebServices {

    @Inject
    public MockSliderAMWebServices(WebAppApi slider) {
      super(slider);
    }

    @Override
    public ManagementResource getManagementResource() {
      return new MockManagementResource(slider);
    }

  }

  public static class MockManagementResource extends ManagementResource {
    public MockManagementResource(WebAppApi slider) {
      super(slider);
    }

    protected AggregateConf getAggregateConf() {
      try {
        JsonSerDeser<ConfTree> confTreeJsonSerDeser = new JsonSerDeser<>(ConfTree.class);
        AggregateConf aggregateConf = new AggregateConf(
            confTreeJsonSerDeser.fromResource(EXAMPLES + "resources.json"),
            confTreeJsonSerDeser.fromResource(EXAMPLES + "app_configuration.json"),
            confTreeJsonSerDeser.fromResource(EXAMPLES + "internal.json")
            );
        aggregateConf.setName("test");
        return aggregateConf;
      } catch (IOException e) {
        throw new AssertionError(e.getMessage(), e);
      }
    }
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    injector = createInjector();
    fs = FileSystem.get(new URI("file:///"), SliderUtils.createConfiguration());
  }

  private static Injector createInjector() {
    return Guice.createInjector(new ServletModule() {
      @Override
      protected void configureServlets() {

        AppState appState = null;
        try {
          fs = FileSystem.get(new URI("file:///"), conf);
          File historyWorkDir = new File("target/history", "TestAMManagementWebServices");
          org.apache.hadoop.fs.Path historyPath =
              new org.apache.hadoop.fs.Path(historyWorkDir.toURI());
          fs.delete(historyPath, true);
          appState = new MockAppState(new MockClusterServices());
          AppStateBindingInfo binding = new AppStateBindingInfo();
          binding.instanceDefinition = factory.newInstanceDefinition(0, 0, 0);
          binding.roles = MockFactory.ROLES;
          binding.fs = fs;
          binding.historyPath = historyPath;
          appState.buildInstance(binding);
        } catch (IOException | BadClusterStateException | URISyntaxException | BadConfigException e) {
          log.error("{}", e, e);
        }
        ProviderAppState providerAppState = new ProviderAppState("undefined",
            appState);

        slider = new WebAppApiImpl(providerAppState,
                                   new MockProviderService(), null, null, null,
            null, null, null);

        bind(SliderJacksonJaxbJsonProvider.class);
        bind(MockSliderAMWebServices.class);
        bind(GenericExceptionHandler.class);
        bind(WebAppApi.class).toInstance(slider);
        bind(Configuration.class).toInstance(conf);

        serve("/*").with(GuiceContainer.class);
      }
    });
  }

  public TestAMManagementWebServices() {
    super(new WebAppDescriptor.Builder(
        "org.apache.hadoop.yarn.appmaster.web")
              .contextListenerClass(GuiceServletConfig.class)
              .filterClass(com.google.inject.servlet.GuiceFilter.class)
              .contextPath("slideram").servletPath("/")
              .clientConfig(
                  new DefaultClientConfig(SliderJacksonJaxbJsonProvider.class))
              .build());
  }

  @Test
  public void testAppResource() throws Exception {
    WebResource r = resource();
    ClientResponse response = r.path("ws").path("v1").path("slider").path("mgmt").path("app")
        .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(200, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    AggregateConfResource json = response.getEntity(AggregateConfResource.class);
    assertEquals("wrong href",
                 "http://localhost:9998/slideram/ws/v1/slider/mgmt/app",
                 json.getHref());
    assertNotNull("no resources", json.getResources());
    assertNotNull("no internal", json.getInternal());
    assertNotNull("no appConf", json.getAppConf());
  }

  @Test
  public void testAppInternal() throws Exception {
    WebResource r = resource();
    ClientResponse
        response =
        r.path("ws").path("v1").path("slider").path("mgmt").path("app").path("configurations").path(
            "internal")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(200, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ConfTreeResource json = response.getEntity(ConfTreeResource.class);
    assertEquals("wrong href",
                 "http://localhost:9998/slideram/ws/v1/slider/mgmt/app/configurations/internal",
                 json.getHref());

    assertDescriptionContains("org/apache/slider/core/conf/examples/internal.json", json);
  }

  private void assertDescriptionContains(String expected, ConfTreeResource json) {

    Map<String, Object> metadata = json.getMetadata();
    assertNotNull("No metadata", metadata);
    Object actual = metadata.get("description");
    assertNotNull("No description", actual);

    assertTrue(String.format("Did not find \"%s\" in \"%s\"", expected, actual),
        actual.toString().contains(expected));
  }

  @Test
  public void testAppResources() throws Exception {
    WebResource r = resource();
    ClientResponse
        response =
        r.path("ws").path("v1").path("slider").path("mgmt").path("app").path("configurations").path(
            "resources")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);

    assertEquals(200, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ConfTreeResource json = response.getEntity(ConfTreeResource.class);
    assertEquals("wrong href",
                 "http://localhost:9998/slideram/ws/v1/slider/mgmt/app/configurations/resources",
                 json.getHref());
    Map<String,Map<String, String>> components = json.getComponents();
    assertNotNull("no components", components);
    assertEquals("incorrect number of components", 2, components.size());
    assertNotNull("wrong component", components.get("worker"));
    assertDescriptionContains("org/apache/slider/core/conf/examples/resources.json", json);
  }

  @Test
  public void testAppAppConf() throws Exception {
    WebResource r = resource();
    ClientResponse
        response =
        r.path("ws").path("v1").path("slider").path("mgmt").path("app").path("configurations").path(
            "appConf")
            .accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
    assertEquals(200, response.getStatus());
    assertEquals(MediaType.APPLICATION_JSON_TYPE, response.getType());
    ConfTreeResource json = response.getEntity(ConfTreeResource.class);
    assertEquals("wrong href",
                 "http://localhost:9998/slideram/ws/v1/slider/mgmt/app/configurations/appConf",
                 json.getHref());
    Map<String,Map<String, String>> components = json.getComponents();
    assertNotNull("no components", components);
    assertEquals("incorrect number of components", 2, components.size());
    assertNotNull("wrong component", components.get("worker"));
    assertDescriptionContains("org/apache/slider/core/conf/examples/app_configuration.json", json);

  }
}
