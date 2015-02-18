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

package org.apache.slider.agent.rest

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.WebResource
import com.sun.jersey.api.client.filter.LoggingFilter
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.client.rest.SliderApplicationApiRestClient

import javax.ws.rs.core.MediaType

import static org.apache.slider.server.appmaster.web.rest.RestPaths.SLIDER_PATH_APPLICATION

/**
 * Uses the Slider Application API for the tests.
 * {@link SliderApplicationApiRestClient}
 */
@CompileStatic
@Slf4j
class RestAPIClientTestDelegates extends AbstractAppApiTestDelegates {
  private SliderApplicationApiRestClient restClient

  /**
   * constructor
   * @param appmaster AM URL
   * @param jersey jersey impl
   * @param enableComplexVerbs flag to enable complex verbs
   */
  RestAPIClientTestDelegates(String appmaster, Client jersey,
      boolean enableComplexVerbs = true) {
    super(enableComplexVerbs, null)
    WebResource amResource = jersey.resource(appmaster)
    amResource.type(MediaType.APPLICATION_JSON)
    def appResource = amResource.path(SLIDER_PATH_APPLICATION);
    // test runs log
    jersey.addFilter(new LoggingFilter(System.out));
    restClient = new SliderApplicationApiRestClient(
        jersey,
        appResource)
    appAPI = restClient
  }

  @Override
  void testPing() {
    super.testPing()
    // test the other verbs
    restClient.pingPut("Put!")
    restClient.pingGet("Get!")
    restClient.pingPost("Post!")
  }

  @Override
  void testFlexOperation() {
    super.testFlexOperation()
  }
}
