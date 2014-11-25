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
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.exceptions.RegistryIOException;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.ExceptionConverter;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.core.registry.docstore.PublishedConfiguration;
import org.apache.slider.core.registry.docstore.PublishedExports;
import org.apache.slider.core.registry.docstore.PublishedExportsSet;
import org.apache.slider.core.registry.info.CustomRegistryConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

/**
 * Registry retriever. 
 * This hides the HTTP operations that take place to
 * get the actual content
 */
public class RegistryRetriever extends AMWebClient {
  private static final Logger log = LoggerFactory.getLogger(RegistryRetriever.class);

  private final String externalConfigurationURL;
  private final String internalConfigurationURL;
  private final String externalExportsURL;
  private final String internalExportsURL;

  public RegistryRetriever(String externalConfigurationURL, String internalConfigurationURL,
                           String externalExportsURL, String internalExportsURL) {
    this.externalConfigurationURL = externalConfigurationURL;
    this.internalConfigurationURL = internalConfigurationURL;
    this.externalExportsURL = externalExportsURL;
    this.internalExportsURL = internalExportsURL;
  }

  /**
   * Retrieve from a service by locating the
   * exported {@link CustomRegistryConstants#PUBLISHER_CONFIGURATIONS_API}
   * and working off it.
   * @param record service record
   * @throws RegistryIOException the address type of the endpoint does
   * not match that expected (i.e. not a list of URLs), missing endpoint...
   */
  public RegistryRetriever(ServiceRecord record) throws RegistryIOException {
    Endpoint internal = record.getInternalEndpoint(
        CustomRegistryConstants.PUBLISHER_CONFIGURATIONS_API);
    String url = null;
    if (internal != null) {
      List<String> addresses = RegistryTypeUtils.retrieveAddressesUriType(
          internal);
      if (addresses != null && !addresses.isEmpty()) {
        url = addresses.get(0);
      }
    }
    internalConfigurationURL = url;
    Endpoint external = record.getExternalEndpoint(
        CustomRegistryConstants.PUBLISHER_CONFIGURATIONS_API);
    url = null;
    if (external != null) {
      List<String> addresses =
          RegistryTypeUtils.retrieveAddressesUriType(external);
      if (addresses != null && !addresses.isEmpty()) {
        url = addresses.get(0);
      }
    }
    externalConfigurationURL = url;

    internal = record.getInternalEndpoint(
        CustomRegistryConstants.PUBLISHER_EXPORTS_API);
    url = null;
    if (internal != null) {
      List<String> addresses = RegistryTypeUtils.retrieveAddressesUriType(
          internal);
      if (addresses != null && !addresses.isEmpty()) {
        url = addresses.get(0);
      }
    }
    internalExportsURL = url;
    external = record.getExternalEndpoint(
        CustomRegistryConstants.PUBLISHER_EXPORTS_API);
    url = null;
    if (external != null) {
      List<String> addresses =
          RegistryTypeUtils.retrieveAddressesUriType(external);
      if (addresses != null && !addresses.isEmpty()) {
        url = addresses.get(0);
      }
    }
    externalExportsURL = url;
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

  protected String getExportURL(boolean external) throws FileNotFoundException {
    String confURL = external ? externalExportsURL: internalExportsURL;
    if (Strings.isStringEmpty(confURL)) {
      throw new FileNotFoundException("No configuration URL");
    }
    return confURL;
  }

  /**
   * Get the configurations of the registry
   * @param external flag to indicate that it is the external entries to fetch
   * @return the configuration sets
   */
  public PublishedExportsSet getExports(boolean external) throws
      FileNotFoundException, IOException {

    String exportsUrl = getExportURL(external);
    try {
      WebResource webResource = jsonResource(exportsUrl);
      log.debug("GET {}", exportsUrl);
      PublishedExportsSet exportSet = webResource.get(PublishedExportsSet.class);
      return exportSet;
    } catch (UniformInterfaceException e) {
      throw ExceptionConverter.convertJerseyException(exportsUrl, e);
    }
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

  /**
   * Get a complete export, with all values
   * @param exportSet
   * @param name name of the configuration
   * @param external flag to indicate that it is an external configuration
   * @return the retrieved config
   * @throws IOException IO problems
   */
  public PublishedExports retrieveExports(PublishedExportsSet exportSet,
                                                      String name,
                                                      boolean external) throws IOException {
    if (!exportSet.contains(name)) {
      throw new FileNotFoundException("Unknown export " + name);
    }
    String exportsURL = getExportURL(external);
    exportsURL = SliderUtils.appendToURL(exportsURL, name);
    try {
      WebResource webResource = jsonResource(exportsURL);
      PublishedExports publishedExports =
          webResource.get(PublishedExports.class);
      return publishedExports;
    } catch (UniformInterfaceException e) {
      throw ExceptionConverter.convertJerseyException(exportsURL, e);
    }
  }

  @Override
  public String toString() {
    return super.toString() 
           + ":  internal URL: \"" + internalConfigurationURL
           + "\";  external \"" + externalConfigurationURL +"\"";
  }
  
  
}
