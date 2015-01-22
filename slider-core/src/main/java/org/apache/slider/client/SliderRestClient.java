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

package org.apache.slider.client;

import com.google.common.base.Preconditions;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.GenericType;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.representation.Form;
import org.apache.hadoop.service.AbstractService;
import org.apache.slider.api.types.SerializedComponentInformation;
import org.apache.slider.api.types.SerializedContainerInformation;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.restclient.HttpVerb;
import org.apache.slider.core.restclient.UgiJerseyBinding;
import org.apache.slider.server.appmaster.web.rest.application.resources.PingResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.Map;

import static org.apache.slider.server.appmaster.web.rest.RestPaths.*;

public class SliderRestClient  extends AbstractService {
  private static final Logger log =
      LoggerFactory.getLogger(SliderRestClient.class);
  private final Client jersey;
  private WebResource appmaster;
  private WebResource appResource;

  public SliderRestClient(String name, Client jersey, WebResource appmaster) {
    super(name);
    Preconditions.checkNotNull(jersey, "null jersey");
    this.jersey = jersey;
  }
  
  public SliderRestClient(Client jersey, WebResource appmaster) {
    this("SliderRestClient", jersey, appmaster);
  }

  public Client getJersey() {
    return jersey;
  }

  public void bindToAppmaster(WebResource appmaster) {
    this.appmaster = appmaster;
    this.appResource = appmaster.path(SLIDER_PATH_APPLICATION);
  }

  public WebResource getAppmaster() {
    return appmaster;
  }

  /**
   * Create a resource under the application path
   * @param subpath
   * @return an resource under the application path
   */
  public WebResource applicationResource(String subpath) {
    return appResource.path(subpath);
  }
  
  /**
   * Get operation against a path under the Application
   * @param <T> type expected
   * @param subpath path
   * @param c class to instantiate
   * @return instance
   * @throws IOException on any problem
   */
  public <T> T getApplicationResource(String subpath, Class<T> c)
      throws IOException {
    return appResourceOperation(HttpVerb.GET, subpath, c);
  } 
  
  /**
   * Get operation against a path under the Application
   * @param <T> type expected
   * @param subpath path
   * @param t type info
   * @return instance
   * @throws IOException on any problem
   */
  public <T> T getApplicationResource(String subpath, GenericType<T> t)
      throws IOException {
    return appResourceOperation(HttpVerb.GET, subpath, t);
  }

  /**
   * 
   * @param method method to exec
   * @param <T> type expected
   * @param subpath path
   * @param c class to instantiate
   * @return instance
   * @throws IOException on any problem
   */
  public <T> T appResourceOperation(HttpVerb method, String subpath, Class<T> c)
      throws IOException {
    WebResource resource = applicationResource(subpath);
    return exec(method, resource, c);
  }
  
  
  /**
   * Get operation against a path under the Application
   * @param <T> type expected
   * @param subpath path
   * @param t type info
   * @return instance
   * @throws IOException on any problem
   */
  public <T> T appResourceOperation(HttpVerb method, String subpath,
      GenericType<T> t)
      throws IOException {
    WebResource resource = applicationResource(subpath);
    return exec(method, resource, t);
  }
  
  /**
   * Execute the operation. Failures are raised as IOException subclasses
   * @param method method to execute
   * @param resource resource to work against
   * @param c class to build
   * @param <T> type expected
   * @return an instance of the type T
   * @throws IOException on any failure
   */
  public <T> T exec(HttpVerb method, WebResource resource, Class<T> c)
      throws IOException {
    try {
      Preconditions.checkArgument(c != null);
      resource.accept(MediaType.APPLICATION_JSON_TYPE);
      return (T) resource.method(method.getVerb(), c);
    } catch (UniformInterfaceException ex) {
      throw UgiJerseyBinding.uprateFaults(method, resource.getURI().toString(),
          ex);
    }
  }
  
  
  /**
   * Execute the operation. Failures are raised as IOException subclasses
   * @param method method to execute
   * @param resource resource to work against
   * @param generic type to work with
   * @param <T> type expected
   * @return an instance of the type T
   * @throws IOException on any failure
   */
  public <T> T exec(HttpVerb method, WebResource resource, GenericType<T> t)
      throws IOException {
    try {
      Preconditions.checkArgument(t != null);
      resource.accept(MediaType.APPLICATION_JSON_TYPE);
      return resource.method(method.getVerb(), t);
    } catch (UniformInterfaceException ex) {
      throw UgiJerseyBinding.uprateFaults(method, resource.getURI().toString(),
          ex);
    }
  }
  
  

  /**
   * Get the aggregate desired model
   * @return the aggregate configuration of what was asked for
   * —before resolution has taken place
   * @throws IOException on any failure
   */
  public AggregateConf getDesiredModel() throws IOException {
    return getApplicationResource(MODEL_DESIRED, AggregateConf.class);
  }


  /**
   * Get the desired application configuration
   * @return the application configuration asked for
   * —before resolution has taken place
   * @throws IOException on any failure
   */
  public ConfTreeOperations getDesiredAppconf() throws IOException {
    ConfTree resource =
        getApplicationResource(MODEL_DESIRED_APPCONF, ConfTree.class);
    return new ConfTreeOperations(resource); 
  }

  /**
   * Get the desired YARN resources
   * @return the resources asked for
   * —before resolution has taken place
   * @throws IOException on any failure
   */
  public ConfTreeOperations getDesiredYarnResources() throws IOException {
    ConfTree resource =
        getApplicationResource(MODEL_DESIRED_RESOURCES, ConfTree.class);
    return new ConfTreeOperations(resource); 
  }

  /**
   * Get the aggregate resolved model
   * @return the aggregate configuration of what was asked for
   * —after resolution has taken place
   * @throws IOException on any failure
   */
  public AggregateConf getResolvedModel() throws IOException {
    return getApplicationResource(MODEL_RESOLVED, AggregateConf.class);
  }


  /**
   * Get the resolved application configuration
   * @return the application configuration asked for
   * —after resolution has taken place
   * @throws IOException on any failure
   */
  public ConfTreeOperations getResolvedAppconf() throws IOException {
    ConfTree resource =
        getApplicationResource(MODEL_RESOLVED_APPCONF, ConfTree.class);
    return new ConfTreeOperations(resource); 
  }

  /**
   * Get the resolved YARN resources
   * @return the resources asked for
   * —after resolution has taken place
   * @throws IOException on any failure
   */
  public ConfTreeOperations getResolvedYarnResources() throws IOException {
    ConfTree resource =
        getApplicationResource(MODEL_RESOLVED_RESOURCES, ConfTree.class);
    return new ConfTreeOperations(resource); 
  }

  /**
   * Get the live YARN resources
   * @return the live set of resources in the cluster
   * @throws IOException on any failure
   */
  public ConfTreeOperations getLiveYarnResources() throws IOException {
    ConfTree resource =
        getApplicationResource(LIVE_RESOURCES, ConfTree.class);
    return new ConfTreeOperations(resource); 
  }

  /**
   * Get a map of live containers [containerId:info]
   * @return a possibly empty list of serialized containers
   * @throws IOException on any failure
   */
  public Map<String, SerializedContainerInformation> enumContainers() throws
      IOException {
    return getApplicationResource(LIVE_RESOURCES,
        new GenericType<Map<String, SerializedContainerInformation>>() {
        });
  }

  /**
   * Get a container from the container Id
   * @param containerId YARN container ID
   * @return the container information
   * @throws IOException on any failure
   */
  public SerializedContainerInformation getContainer( String containerId) throws
      IOException {
    return getApplicationResource(LIVE_CONTAINERS + "/" + containerId,
        SerializedContainerInformation.class);
  }

  /**
   * List all components into a map of [name:info]
   * @return a possibly empty map of components
   * @throws IOException on any failure
   */
  public Map<String, SerializedComponentInformation> enumComponents() throws
      IOException {
    return getApplicationResource(LIVE_COMPONENTS,
        new GenericType<Map<String, SerializedComponentInformation>>() {
        });
  }

  /**
   * Get information about a component
   * @param componentName name of the component
   * @return the component details
   * @throws IOException on any failure
   */
  public SerializedComponentInformation getComponent(String componentName) throws
      IOException {
    return getApplicationResource(LIVE_COMPONENTS + "/" + componentName,
        SerializedComponentInformation.class);
  }

  /**
   * Ping as a post
   * @param text text to include
   * @return the response
   * @throws IOException on any failure
   */
  public PingResource ping(String text) throws IOException {
    WebResource pingOut = applicationResource(ACTION_PING);
    pingOut.accept(MediaType.APPLICATION_JSON_TYPE);
    pingOut.type(MediaType.APPLICATION_JSON_TYPE);
    Form f = new Form();
    f.add("text", text);
    return pingOut.post(PingResource.class, f);
  }

}
