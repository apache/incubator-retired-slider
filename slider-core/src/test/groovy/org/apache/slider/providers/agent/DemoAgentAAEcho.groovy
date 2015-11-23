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

package org.apache.slider.providers.agent

import org.apache.slider.client.SliderClient

/**
 * Overridde the test actions with a sleep command, so that developers
 * can see what the application is up to
 */
class DemoAgentAAEcho extends TestAgentAAEcho {

  @Override
  protected void postLaunchActions(
      SliderClient sliderClient,
      String clustername,
      String rolename,
      Map<String, Integer> roles,
      String proxyAM) {

    def url = proxyAM
    // spin repeating the URl in the logs so YARN chatter doesn't lose it
    describe("Web UI is at $url")

    // run the superclass rest tests
  //  queryRestAPI(sliderClient, roles, proxyAM)

    5.times {
      describe("Web UI is at $url")
      sleep(60 *1000)
    }
  }
}
