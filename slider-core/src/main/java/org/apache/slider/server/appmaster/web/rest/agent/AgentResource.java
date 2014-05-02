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
package org.apache.slider.server.appmaster.web.rest.agent;

import org.apache.slider.server.appmaster.web.WebAppApi;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class AgentResource {

  private final WebAppApi slider;
  private String agent_name;

  public AgentResource(WebAppApi slider) {
    this.slider = slider;
  }

  private void init(HttpServletResponse res) {
    res.setContentType(null);
  }

  @GET
  @Path("/agents/register")
  public Response endpointAgentRegister() {
    Response response = Response.status(200).entity("/agent/register").build();
    return response;
  }

  @GET
  @Path("/agents")
  public Response endpointAgent() {
    Response response = Response.status(200).entity("/agent").build();
    return response;
  }
  @GET
  @Path("/")
  public Response endpointRoot() {
    Response response = Response.status(200).entity("/").build();
    return response;
  }

  @POST
  @Path("/{agent_name: [a-zA-Z][a-zA-Z_0-9]*}/register")
  @Consumes({MediaType.APPLICATION_JSON})
  @Produces({MediaType.APPLICATION_JSON})
  public RegistrationResponse register(Register registration,
                                       @Context HttpServletResponse res,
                                       @PathParam("agent_name") String agent_name) {
    init(res);
    this.agent_name = agent_name;
    AgentRestOperations ops = slider.getAgentRestOperations();
    return ops.handleRegistration(registration);

  }

  @POST
  @Path("/{agent_name: [a-zA-Z][a-zA-Z_0-9]*}/heartbeat")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces({MediaType.APPLICATION_JSON})
  public HeartBeatResponse heartbeat(HeartBeat message,
                                     @Context HttpServletResponse res,
                                     @PathParam("agent_name") String agent_name) {
    init(res);
    AgentRestOperations ops = slider.getAgentRestOperations();
    return ops.handleHeartBeat(message);
  }
}
