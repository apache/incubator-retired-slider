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

package org.apache.slider.server.appmaster.web.rest.application;

import com.google.common.collect.Lists;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.slider.api.types.SerializedComponentInformation;
import org.apache.slider.api.types.SerializedContainerInformation;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.server.appmaster.state.RoleInstance;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.rest.AbstractSliderResource;
import org.apache.slider.server.appmaster.web.rest.RestPaths;
import org.apache.slider.server.appmaster.web.rest.application.resources.CachedContent;
import org.apache.slider.server.appmaster.web.rest.application.resources.LiveContainersRefresher;
import org.apache.slider.server.appmaster.web.rest.application.resources.ContentCache;
import org.apache.slider.server.appmaster.web.rest.application.resources.LiveComponentsRefresher;
import org.apache.slider.server.appmaster.web.rest.application.resources.LiveResourcesRefresher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
@SuppressWarnings("unchecked")
public class ApplicationResource extends AbstractSliderResource {
  private static final Logger log =
      LoggerFactory.getLogger(ApplicationResource.class);

  public static final int LIFESPAN = 1000;
  public static final List<String> LIVE_ENTRIES = toJsonList("resources",
      "containers",
      "components",
      "nodes",
      "statistics",
      "internal");
  public static final List<String> ROOT_ENTRIES =
      toJsonList("model", "live", "actions");
  private final ContentCache cache = new ContentCache();
  private final StateAccessForProviders state;

  public ApplicationResource(WebAppApi slider) {
    super(slider);
    state = slider.getAppState();
    cache.put(RestPaths.LIVE_RESOURCES,
        new CachedContent<ConfTree>(LIFESPAN,
            new LiveResourcesRefresher(state)));
    cache.put(RestPaths.LIVE_CONTAINERS,
        new CachedContent<Map<String, SerializedContainerInformation>>(LIFESPAN,
            new LiveContainersRefresher(state)));
    cache.put(RestPaths.LIVE_COMPONENTS,
        new CachedContent<Map<String, SerializedComponentInformation>> (LIFESPAN,
            new LiveComponentsRefresher(state)));
  }

  /**
   * Build a new JSON-marshallable list of string elements
   * @param elements elements
   * @return something that can be returned
   */
  private static List<String> toJsonList(String... elements) {
    return Lists.newArrayList(elements);
  }

  @GET
  @Path("/")
  @Produces({MediaType.APPLICATION_JSON})
  public List<String> getRoot() {
    return ROOT_ENTRIES;
  }

  @GET
  @Path("/model")
  @Produces({MediaType.APPLICATION_JSON})
  public List<String> getModel() {
    return toJsonList("desired", "resolved");
  }

  @GET
  @Path("/live")
  @Produces({MediaType.APPLICATION_JSON})
  public List<String> getLive() {
    return LIVE_ENTRIES;
  }

  @GET
  @Path(RestPaths.LIVE_RESOURCES)
  @Produces({MediaType.APPLICATION_JSON})
  public Object getLiveResources() {
    try {
      return cache.lookup(RestPaths.LIVE_RESOURCES);
    } catch (Exception e) {
      throw buildException(RestPaths.LIVE_RESOURCES, e);
    }
  }
  
  @GET
  @Path(RestPaths.LIVE_CONTAINERS)
  @Produces({MediaType.APPLICATION_JSON})
  public Map<String, SerializedContainerInformation> getLiveContainers() {
    try {
      return (Map<String, SerializedContainerInformation>)cache.lookup(
          RestPaths.LIVE_CONTAINERS);
    } catch (Exception e) {
      throw buildException(RestPaths.LIVE_CONTAINERS, e);
    }
  }

  @GET
  @Path(RestPaths.LIVE_CONTAINERS + "/{containerId}")
  @Produces({MediaType.APPLICATION_JSON})
  public SerializedContainerInformation getLiveContainer(
      @PathParam("containerId") String containerId) {
    try {
      RoleInstance id = state.getLiveInstanceByContainerID(containerId);
      return id.serialize();
    } catch (NoSuchNodeException e) {
      throw new NotFoundException("Unknown container: " + containerId);
    } catch (Exception e) {
      throw buildException(RestPaths.LIVE_CONTAINERS + "/"+ containerId, e);
    }
  }

  @GET
  @Path(RestPaths.LIVE_COMPONENTS)
  @Produces({MediaType.APPLICATION_JSON})
  public Map<String, SerializedComponentInformation> getLiveComponents() {
    try {
      return (Map<String, SerializedComponentInformation>) cache.lookup(
          RestPaths.LIVE_COMPONENTS);
    } catch (Exception e) {
      throw buildException(RestPaths.LIVE_COMPONENTS, e);
    }
  }
  
  @GET
  @Path(RestPaths.LIVE_COMPONENTS+"/{component}")
  @Produces({MediaType.APPLICATION_JSON})
  public SerializedComponentInformation getLiveComponent(
      @PathParam("component") String component) {
    try {
      RoleStatus roleStatus = state.lookupRoleStatus(component);
      SerializedComponentInformation info = roleStatus.serialize();
      List<RoleInstance> containers = lookupRoleContainers(component);
      info.containers = new ArrayList<String>(containers.size());
      for (RoleInstance container : containers) {
        info.containers.add(container.id);
      }
      return info;
    } catch (YarnRuntimeException e) {
      throw new NotFoundException("Unknown component: " + component);
    } catch (Exception e) {
      throw buildException(RestPaths.LIVE_CONTAINERS, e);
    }
  }
  
  List<RoleInstance> lookupRoleContainers(String component) {
    RoleStatus roleStatus = state.lookupRoleStatus(component);
    List<RoleInstance> ownedContainerList = state.cloneOwnedContainerList();
    List<RoleInstance> matching = new ArrayList<RoleInstance>(ownedContainerList.size());
    int roleId = roleStatus.getPriority();
    for (RoleInstance instance : ownedContainerList) {
      if (instance.roleId == roleId) {
        matching.add(instance);
      }
    }
    return matching;
  }
  
}
