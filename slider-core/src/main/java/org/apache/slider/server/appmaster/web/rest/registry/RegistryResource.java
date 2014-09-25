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

package org.apache.slider.server.appmaster.web.rest.registry;

import com.google.inject.Singleton;
import org.apache.hadoop.fs.PathAccessDeniedException;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.yarn.registry.client.api.RegistryOperations;
import org.apache.hadoop.yarn.registry.client.exceptions.AuthenticationFailedException;
import org.apache.hadoop.yarn.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.yarn.webapp.ForbiddenException;
import org.apache.hadoop.yarn.webapp.NotFoundException;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.util.List;

/**
 * This is the read-only view of the slider YARN registry
 * 
 * Model:
 * <ol>
 *   <li>a tree of nodes</li>
 *   <li>Default view is of children + record</li>
 * </ol>
 * 
 */
@Singleton
//@Path(RestPaths.SLIDER_PATH_REGISTRY)
public class RegistryResource {
  protected static final Logger log =
      LoggerFactory.getLogger(RegistryResource.class);
  public static final String SERVICE_PATH =
      "/{path:.*}";

  private final RegistryOperations registry;

  /**
   * Construct an instance bonded to a registry
   * @param slider slider API
   */
  public RegistryResource(WebAppApi slider) {
    this.registry = slider.getRegistryOperations();
  }

  /**
   * Internal init code, per request
   * @param request incoming request 
   * @param uriInfo URI details
   */
  private void init(HttpServletRequest request, UriInfo uriInfo) {
    log.debug(uriInfo.getRequestUri().toString());
  }

  @GET
  public Response getRoot(@Context HttpServletRequest request) {
    return Response.ok("registry root").build();
  }


//   {path:.*}

  @Path(SERVICE_PATH)
  @GET
  @Produces({MediaType.APPLICATION_JSON})
  public Response lookup(
      @PathParam("path") String path,
      @Context HttpServletRequest request,
      @Context UriInfo uriInfo) {
    try {
      init(request, uriInfo);
      List<RegistryPathStatus> list = registry.listFull(path);
      return Response.ok("found").build();
    } catch (PathNotFoundException e) {
      throw new NotFoundException(path);
    } catch (AuthenticationFailedException e) {
      throw new ForbiddenException(path);
    } catch (PathAccessDeniedException e) {
      throw new ForbiddenException(path);
    } catch (Exception e) {
      return fromException(e);
    }
  }

  /**
   * Handle an exception
   * @param e exception
   * @return a response to return
   */
  Response fromException(Exception e) {
    log.error("Error during generation of response: {}", e, e);
    if (e instanceof PathNotFoundException) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    if (e instanceof AuthenticationFailedException
        || e instanceof PathAccessDeniedException) {
      return Response.status(Response.Status.FORBIDDEN).build();
    }
    return Response.serverError().build();
  }
  
  
}
