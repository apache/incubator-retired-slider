/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.server.appmaster.web;

import com.google.common.base.Preconditions;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.core.util.FeaturesAndProperties;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.hadoop.yarn.webapp.Dispatcher;
import org.apache.hadoop.yarn.webapp.GenericExceptionHandler;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.slider.core.registry.info.ServiceInstanceData;
import org.apache.slider.server.appmaster.web.rest.AMWadlGeneratorConfig;
import org.apache.slider.server.appmaster.web.rest.AMWebServices;
import org.apache.slider.server.appmaster.web.rest.SliderJacksonJaxbJsonProvider;
import org.apache.slider.server.services.curator.CuratorHelper;
import org.apache.slider.server.services.curator.RegistryBinderService;
import org.apache.slider.server.services.curator.RegistryDiscoveryContext;
import org.apache.slider.server.services.curator.RegistryRestResources;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 */
public class SliderAMWebApp extends WebApp {
  public static final String BASE_PATH = "slideram";
  public static final String CONTAINER_STATS = "/stats";
  public static final String CLUSTER_SPEC = "/spec";

  public final RegistryBinderService<ServiceInstanceData> registry;

  public SliderAMWebApp(RegistryBinderService<ServiceInstanceData> registry) {
    Preconditions.checkNotNull(registry);
    this.registry = registry;
  }

  @Override
  public void setup() {
    Logger.getLogger("com.sun.jersey").setLevel(Level.FINEST);
    // Make one of these to ensure that the jax-b annotations
    // are properly picked up.
    bind(SliderJacksonJaxbJsonProvider.class);
    
    // Get exceptions printed to the screen
    bind(GenericExceptionHandler.class);
    // bind the REST interface
    bind(AMWebServices.class);
    //bind(AMAgentWebServices.class);

    CuratorHelper curatorHelper = registry.getCuratorHelper();
    ServiceDiscovery<ServiceInstanceData> discovery = registry.getDiscovery();
    RegistryDiscoveryContext discoveryContext = curatorHelper
                                                        .createDiscoveryContext(
                                                          discovery);

    bind(RegistryDiscoveryContext.class).toInstance(discoveryContext);
    RegistryRestResources registryRestResources =
      new RegistryRestResources(discoveryContext, registry);
    bind(RegistryRestResources.class).toInstance(registryRestResources);

    route("/", SliderAMController.class);
    route(CONTAINER_STATS, SliderAMController.class, "containerStats");
    route(CLUSTER_SPEC, SliderAMController.class, "specification");
  }

  @Override
  public void configureServlets() {
    setup();

    serve("/", "/__stop").with(Dispatcher.class);

    for (String path : this.getServePathSpecs()) {
      serve(path).with(Dispatcher.class);
    }

    String regex = "(?!/ws)";
    serveRegex(regex).with(SliderDefaultWrapperServlet.class);

    Map<String, String> params = new HashMap<String, String>();
    params.put(ResourceConfig.FEATURE_IMPLICIT_VIEWABLES, "true");
    params.put(ServletContainer.FEATURE_FILTER_FORWARD_ON_404, "true");
    params.put(FeaturesAndProperties.FEATURE_XMLROOTELEMENT_PROCESSING, "true");
    params.put(ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS, GZIPContentEncodingFilter.class.getName());
    params.put(ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS, GZIPContentEncodingFilter.class.getName());
    //params.put("com.sun.jersey.spi.container.ContainerRequestFilters", "com.sun.jersey.api.container.filter.LoggingFilter");
    //params.put("com.sun.jersey.spi.container.ContainerResponseFilters", "com.sun.jersey.api.container.filter.LoggingFilter");
    //params.put("com.sun.jersey.config.feature.Trace", "true");
    params.put("com.sun.jersey.config.property.WadlGeneratorConfig",
        AMWadlGeneratorConfig.CLASSNAME);
    filter("/*").through(GuiceContainer.class, params);
  }
}
