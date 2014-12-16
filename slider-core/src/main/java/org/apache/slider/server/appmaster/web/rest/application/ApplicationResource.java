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
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.server.appmaster.web.WebAppApi;
import org.apache.slider.server.appmaster.web.rest.AbstractSliderResource;
import org.apache.slider.server.appmaster.web.rest.RestPaths;
import org.apache.slider.server.appmaster.web.rest.application.resources.CachedContent;
import org.apache.slider.server.appmaster.web.rest.application.resources.ContentCache;
import org.apache.slider.server.appmaster.web.rest.application.resources.LiveResourcesRefresher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;

public class ApplicationResource extends AbstractSliderResource {
  private static final Logger log =
      LoggerFactory.getLogger(ApplicationResource.class);

  public static final int LIFESPAN = 1000;
  private final ContentCache cache = new ContentCache();

  public ApplicationResource(WebAppApi slider) {
    super(slider);
    cache.put(RestPaths.LIVE_RESOURCES,
        new CachedContent<ConfTree>(LIFESPAN,
            new LiveResourcesRefresher(slider.getAppState())));
  }

  /**
   * Build a new JSON-marshallable list of string elements
   * @param elements elements
   * @return something that can be returned
   */
  private List<String> toJsonList(String... elements) {
    return Lists.newArrayList(elements);
  }

  @GET
  @Path("/")
  @Produces({MediaType.APPLICATION_JSON})
  public List<String> getRoot() {
    return toJsonList("model", "live", "actions");
  }

  @GET
  @Path("/model")
  @Produces({MediaType.APPLICATION_JSON})
  public List<String> getModel() {
    return toJsonList("desired", "resolved");
  }

  @GET
  @Path("/model/")
  @Produces({MediaType.APPLICATION_JSON})
  public List<String> getModelSlash() {
    return getModel();
  }

  @GET
  @Path("/live")
  @Produces({MediaType.APPLICATION_JSON})
  public List<String> getLive() {
    return toJsonList("resources",
        "containers",
        "components",
        "nodes",
        "statistics",
        "internal");
  }

  @GET
  @Path("/live/")
  @Produces({MediaType.APPLICATION_JSON})
  public List<String> getLiveSlash() {
    return getLive();
  }

  @GET
  @Path(RestPaths.LIVE_RESOURCES)
  @Produces({MediaType.APPLICATION_JSON})
  public Object getLiveResources() {
    return cache.get(RestPaths.LIVE_RESOURCES).get();
  }

}
