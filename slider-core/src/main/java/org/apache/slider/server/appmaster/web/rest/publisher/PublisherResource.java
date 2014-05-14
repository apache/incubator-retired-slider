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

package org.apache.slider.server.appmaster.web.rest.publisher;

import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.slider.core.registry.docstore.ConfigFormat;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.docstore.PublishedConfigurationOutputter;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static  org.apache.slider.server.appmaster.web.rest.RestPaths.*;

/**
 * This publishes configuration sets
 */
public class PublisherResource {
  protected static final Logger log =
      LoggerFactory.getLogger(PublisherResource.class);
  private final WebAppApi slider;
  private static final String CONFIG =
      "{config: " + PUBLISHED_CONFIGURATION_REGEXP + "}";
  
  public PublisherResource(WebAppApi slider) {
    this.slider = slider;
  }

  private void init(HttpServletResponse res, UriInfo uriInfo) {
    res.setContentType(null);
    log.debug(uriInfo.getRequestUri().toString());
  }

  private PublishedConfigSet getContent() {
    return slider.getAppState().getPublishedConfigurations();
  }

  @GET
  @Path("/")
  @Produces({MediaType.APPLICATION_JSON})
  public PublishedConfigSet getPublishedConfiguration(
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) {
    init(res, uriInfo);

    PublishedConfigSet publishedConfigSet = getContent();
    log.debug("number of available configurations: {}", publishedConfigSet.size());
    return publishedConfigSet.shallowCopy();
  }

  private void logRequest(UriInfo uriInfo) {
    log.debug(uriInfo.getRequestUri().toString());
  }

  @GET
  @Path("/" + CONFIG)
  @Produces({MediaType.APPLICATION_JSON})
  public PublishedConfiguration getConfigurationInstance(
      @PathParam("config") String config,
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) {
    init(res, uriInfo);

    PublishedConfiguration publishedConfig = getContent().get(config);
    if (publishedConfig == null) {
      log.info("Configuration {} not found", config);
      throw new NotFoundException("Not found: " + uriInfo.getAbsolutePath());
    }
    return publishedConfig;
  }
  
  @GET
  @Path("/" + CONFIG+ ".json")
  @Produces({MediaType.APPLICATION_JSON})
  public String getConfigurationContentJson(
      @PathParam("config") String config,
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) throws IOException {
    return getStringRepresentation(config, uriInfo, res,
        ConfigFormat.JSON);
  }

  @GET
  @Path("/" + CONFIG + ".xml")
  @Produces({MediaType.APPLICATION_XML})
  public String getConfigurationContentXML(
      @PathParam("config") String config,
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) throws IOException {
    return getStringRepresentation(config, uriInfo, res,
        ConfigFormat.XML);
  }
  
  @GET
  @Path("/" + CONFIG + ".properties")
  @Produces({MediaType.APPLICATION_XML})
  public String getConfigurationContentProperties(
      @PathParam("config") String config,
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) throws IOException {

    return getStringRepresentation(config, uriInfo, res,
        ConfigFormat.PROPERTIES);
  }

  public String getStringRepresentation(String config,
      UriInfo uriInfo,
      HttpServletResponse res, ConfigFormat format) throws IOException {
    // delegate (including init)
    PublishedConfiguration publishedConfig =
        getConfigurationInstance(config, uriInfo, res);
    PublishedConfigurationOutputter outputter =
        publishedConfig.createOutputter(format);
    return outputter.asString();
  }

  @GET
  @Path("/{config}/{propertyName}")
  @Produces({MediaType.APPLICATION_JSON})
  public Map<String,String> getConfigurationProperty(
      @PathParam("propertyName") String propertyName,
      @PathParam("config") String config,
      @Context UriInfo uriInfo,
      @Context HttpServletResponse res) {
    PublishedConfiguration publishedConfig =
        getConfigurationInstance(config, uriInfo, res);
    String propVal = publishedConfig.entries.get(propertyName);
    if (propVal == null) {
      log.info("Configuration property {} not found in configuration {}",
               propertyName, config);
      throw new NotFoundException("Property not found: " + propertyName);
    }
    Map<String,String> rtnVal = new HashMap<>();
    rtnVal.put(propertyName, propVal);

    return rtnVal;
  }
  
}
