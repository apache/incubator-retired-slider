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

package org.apache.slider.core.registry.retrieve;

import com.beust.jcommander.Strings;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.json.JSONConfiguration;
import org.apache.hadoop.yarn.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.yarn.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.yarn.registry.client.types.Endpoint;
import org.apache.hadoop.yarn.registry.client.types.ServiceRecord;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.ExceptionConverter;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.info.CustomRegistryConstants;
import org.apache.slider.core.registry.info.ServiceInstanceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Registry retriever. 
 * This hides the HTTP operations that take place to
 * get the actual content
 */
public class RegistryRetriever {
  private static final Logger log = LoggerFactory.getLogger(RegistryRetriever.class);

  private final String externalConfigurationURL;
  private final String internalConfigurationURL;
  private static final Client jerseyClient;
  
  static {
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put(
        JSONConfiguration.FEATURE_POJO_MAPPING,
        Boolean.TRUE);
    jerseyClient = Client.create(clientConfig);
    jerseyClient.setFollowRedirects(true);
  }


  public RegistryRetriever(ServiceInstanceData instance) {
    externalConfigurationURL =  instance.externalView.configurationsURL;    
    internalConfigurationURL =  instance.internalView.configurationsURL;    
  }

  
  public RegistryRetriever(String externalConfigurationURL, String internalConfigurationURL) {
    this.externalConfigurationURL = externalConfigurationURL; 
    this.internalConfigurationURL = internalConfigurationURL; 
  }

  public RegistryRetriever(ServiceRecord record) throws InvalidRecordException {
    Endpoint internal = record.getInternalEndpoint(
        CustomRegistryConstants.PUBLISHER_CONFIGURATIONS_API);
    
    internalConfigurationURL = RegistryTypeUtils.retrieveAddressUriType(
        internal);
    Endpoint external = record.getExternalEndpoint(
        CustomRegistryConstants.PUBLISHER_CONFIGURATIONS_API);

    externalConfigurationURL = RegistryTypeUtils.retrieveAddressUriType(
        external);
  }


  /**
   * Does a bonded registry retriever have a configuration?
   * @param external flag to indicate that it is the external entries to fetch
   * @return true if there is a URL to the configurations defined
   */
  public boolean hasConfigurations(boolean external) {
    return !Strings.isStringEmpty(
        external ? externalConfigurationURL : internalConfigurationURL);
  }
  
  /**
   * Get the configurations of the registry
   * @param external flag to indicate that it is the external entries to fetch
   * @return the configuration sets
   */
  public PublishedConfigSet getConfigurations(boolean external) throws
      FileNotFoundException, IOException {

    String confURL = getConfigurationURL(external);
    try {
      WebResource webResource = jsonResource(confURL);
      log.debug("GET {}", confURL);
      PublishedConfigSet configSet = webResource.get(PublishedConfigSet.class);
      return configSet;
    } catch (UniformInterfaceException e) {
      throw ExceptionConverter.convertJerseyException(confURL, e);
    }
  }

  protected String getConfigurationURL(boolean external) throws FileNotFoundException {
    String confURL = external ? externalConfigurationURL: internalConfigurationURL;
    if (Strings.isStringEmpty(confURL)) {
      throw new FileNotFoundException("No configuration URL");
    }
    return confURL;
  }

  private WebResource resource(String url) {
    WebResource resource = jerseyClient.resource(url);
    return resource;
  }

  private WebResource jsonResource(String url) {
    WebResource resource = resource(url);
    resource.type(MediaType.APPLICATION_JSON);
    return resource;
  }

  /**
   * Get a complete configuration, with all values
   * @param configSet config set to ask for
   * @param name name of the configuration
   * @param external flag to indicate that it is an external configuration
   * @return the retrieved config
   * @throws IOException IO problems
   */
  public PublishedConfiguration retrieveConfiguration(PublishedConfigSet configSet,
      String name,
      boolean external) throws IOException {
    String confURL = getConfigurationURL(external);
    if (!configSet.contains(name)) {
      throw new FileNotFoundException("Unknown configuration " + name);
    }
    confURL = SliderUtils.appendToURL(confURL, name);
    try {
      WebResource webResource = jsonResource(confURL);
      PublishedConfiguration publishedConf =
          webResource.get(PublishedConfiguration.class);
      return publishedConf;
    } catch (UniformInterfaceException e) {
      throw ExceptionConverter.convertJerseyException(confURL, e);
    }
  }
  
  @Override
  public String toString() {
    return super.toString() 
           + " - external " + externalConfigurationURL;
  }
  
  
}
