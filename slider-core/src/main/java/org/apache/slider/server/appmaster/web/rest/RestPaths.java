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

/**
 * Paths in the REST App
 */
public class RestPaths {

  public static final String WS_CONTEXT_ROOT = "/ws";
  public static final String SLIDER_CONTEXT_ROOT = WS_CONTEXT_ROOT +"/v1/slider";
  public static final String SLIDER_SUBPATH_MANAGEMENT = "/mgmt";
  public static final String SLIDER_SUBPATH_AGENTS = "/agents";
  public static final String SLIDER_SUBPATH_PUBLISHER = "/publisher";

  public static final String SLIDER_PATH_MANAGEMENT = SLIDER_CONTEXT_ROOT
                                      + SLIDER_SUBPATH_MANAGEMENT;
  public static final String SLIDER_PATH_AGENTS = SLIDER_CONTEXT_ROOT
                                      + SLIDER_SUBPATH_AGENTS;
  
  public static final String SLIDER_PATH_PUBLISHER = SLIDER_CONTEXT_ROOT
                                      + SLIDER_SUBPATH_PUBLISHER;

  public static final String SLIDER_SUBPATH_REGISTRY = "/registry";
  public static final String SLIDER_PATH_REGISTRY = WS_CONTEXT_ROOT
                                                    + SLIDER_SUBPATH_REGISTRY;

  public static final String REGISTRY_SERVICE = "v1/service";
  public static final String REGISTRY_ANYSERVICE = "v1/anyservice";
}
