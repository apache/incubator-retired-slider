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

import com.sun.jersey.api.client.Client
import com.sun.jersey.api.client.config.ClientConfig
import com.sun.jersey.api.client.config.DefaultClientConfig
import com.sun.jersey.api.json.JSONConfiguration
import org.apache.slider.server.appmaster.web.rest.agent.Register

class AgentTestUtils {

  public static Client createTestClient() {
    ClientConfig clientConfig = new DefaultClientConfig();
    clientConfig.getFeatures().put(
        JSONConfiguration.FEATURE_POJO_MAPPING,
        Boolean.TRUE);
    return Client.create(clientConfig);
  }

  public static Register createDummyJSONRegister() {
    Register register = new Register();
    register.setResponseId(-1);
    register.setTimestamp(System.currentTimeMillis());
    register.setLabel("dummyHost");
    return register;
  }
}
