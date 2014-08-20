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

package org.apache.slider.server.services.registry;

import com.google.inject.Singleton;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.server.rest.DiscoveryContext;
import org.apache.curator.x.discovery.server.rest.DiscoveryResource;
import org.apache.slider.core.registry.info.ServiceInstanceData;
import org.apache.slider.server.appmaster.web.rest.RestPaths;
import org.apache.slider.server.services.curator.CuratorServiceInstance;
import org.apache.slider.server.services.curator.CuratorServiceInstances;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Random;

@Singleton
@Path(RestPaths.SLIDER_PATH_REGISTRY)
public class RegistryRestResources extends DiscoveryResource<ServiceInstanceData> {
  public static final String SERVICE_NAME = RestPaths.REGISTRY_SERVICE +"/{name}";
  public static final String SERVICE_NAME_ID = SERVICE_NAME + "/{id}";
  protected static final Logger log =
      LoggerFactory.getLogger(RegistryRestResources.class);
  private final SliderRegistryService registry;
  private DiscoveryContext<ServiceInstanceData> context;
  private Random randomizer = new Random();

  public RegistryRestResources(@Context DiscoveryContext<ServiceInstanceData> context,
      SliderRegistryService registry) {
    super(context);
    this.context = context;
    this.registry = registry;
  }

//  @GET
  public Response getWadl (@Context HttpServletRequest request) {
    try {
      URI location = new URL(request.getScheme(),
                                      request.getServerName(),
                                      request.getServerPort(),
                                      "/application.wadl").toURI();
      return Response.temporaryRedirect(location).build();
    } catch (Exception e) {
      log.error("Error during redirect to WADL", e);
      throw new WebApplicationException(Response.serverError().build());
    }
  }

  @javax.ws.rs.GET
  @javax.ws.rs.Produces({MediaType.APPLICATION_JSON})
  public Response getAtRoot() {
    try {
      List<String>
          instances = registry.serviceTypes();
      return Response.ok(instances).build();
    } catch (Exception e) {
      log.error("Error during generation of response", e);
      return Response.serverError().build();
    }
  }


    @Override
  @javax.ws.rs.GET
  @javax.ws.rs.Path(SERVICE_NAME)
  @javax.ws.rs.Produces({MediaType.APPLICATION_JSON})
  public Response getAll(@PathParam("name") String name) {
    try {
      List<CuratorServiceInstance<ServiceInstanceData>>
          instances = registry.listInstances(name);
      return Response.ok(
      new CuratorServiceInstances<ServiceInstanceData>(instances)).build();
    } catch (Exception e) {
      log.error("Error during generation of response", e);
      return Response.serverError().build();
    }
  }

  @Override
  @GET
  @Path(SERVICE_NAME_ID)
  @Produces(MediaType.APPLICATION_JSON)
  public Response get(@PathParam("name") String name,
                      @PathParam("id") String id) {
    try {
      CuratorServiceInstance<ServiceInstanceData> instance = registry.queryForInstance(name, id);
      if ( instance == null )
      {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      Response.ResponseBuilder builder = Response.ok(instance);
      return builder.build();
    } catch (Exception e) {
      log.error("Trying to get instance {} from service {}: {})",
          id,
          name,
          e);
      return Response.serverError().build();
    }
  }

  @Override
  @GET
  @Path("v1/anyservice/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAny(@PathParam("name") String name) {
    try {
      List<CuratorServiceInstance<ServiceInstanceData>>
          instances = registry.listInstances(name);
      if (instances == null || instances.isEmpty()) {
        return Response.status(Response.Status.NOT_FOUND).build();
      }

      CuratorServiceInstance<ServiceInstanceData> randomInstance =
          instances.get(randomizer.nextInt(instances.size()));
      if ( randomInstance == null )
      {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
      return Response.ok(randomInstance).build();
    } catch (Exception e) {
      log.error(String.format("Trying to get any instance from service (%s)", name), e);
      return Response.serverError().build();
    }
  }

  @Override
  @PUT
  @Path(SERVICE_NAME_ID)
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response putService(ServiceInstance<ServiceInstanceData> instance,
                             @PathParam("name") String name,
                             @PathParam("id") String id) {
    throw new UnsupportedOperationException("putService not supported");
  }

  @Override
  @DELETE
  @Path(SERVICE_NAME_ID)
  public Response removeService(@PathParam("name") String name,
                                @PathParam("id") String id) {
    throw new UnsupportedOperationException("removeService not supported");
  }
}

