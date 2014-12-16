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

package org.apache.slider.server.appmaster.web.rest;

import org.apache.slider.server.appmaster.web.WebAppApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URL;

/**
 * Abstract resource base class for REST resources
 * that use the slider WebAppApi
 */
public abstract class AbstractSliderResource {
  private static final Logger log =
      LoggerFactory.getLogger(AbstractSliderResource.class);
  protected final WebAppApi slider;

  public AbstractSliderResource(WebAppApi slider) {
    this.slider = slider;
  }


  /**
   * Generate a redirect to the WASL
   * @param request to base the URL on
   * @return a 302 response
   */
  protected Response redirectToAppWadl(HttpServletRequest request) {
    try {
      URI location = new URL(request.getScheme(),
          request.getServerName(),
          request.getServerPort(),
          RestPaths.APPLICATION_WADL).toURI();
      return Response.temporaryRedirect(location).build();
    } catch (Exception e) {
      log.error("Error during redirect to WADL", e);
      throw new WebApplicationException(Response.serverError().build());
    }
  }

}
