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

package org.apache.slider.server.appmaster.web.rest.application.resources;

import org.apache.slider.api.types.SerializedComponentInformation;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.apache.slider.server.appmaster.state.StateAccessForProviders;

import java.util.HashMap;
import java.util.Map;

public class LiveComponentsRefresher
    implements ResourceRefresher<Map<String, SerializedComponentInformation>> {

  private final StateAccessForProviders state;

  public LiveComponentsRefresher(StateAccessForProviders state) {
    this.state = state;
  }

  @Override
  public Map<String, SerializedComponentInformation> refresh() throws
      Exception {

    Map<Integer, RoleStatus> roleStatusMap = state.getRoleStatusMap();
    Map<String, SerializedComponentInformation> results =
        new HashMap<String, SerializedComponentInformation>(
            roleStatusMap.size());

    for (RoleStatus status : roleStatusMap.values()) {
      String name = status.getName();
      SerializedComponentInformation info = status.serialize();
      results.put(name, info);
    }
    return results;
  }
}
