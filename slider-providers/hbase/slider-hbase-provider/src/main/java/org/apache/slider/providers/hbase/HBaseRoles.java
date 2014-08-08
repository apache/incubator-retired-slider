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

package org.apache.slider.providers.hbase;

import org.apache.slider.common.SliderKeys;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;

import java.util.ArrayList;
import java.util.List;

public class HBaseRoles {

  /**
   * List of roles
   */
  protected static final List<ProviderRole> ROLES =
    new ArrayList<ProviderRole>();

  public static final int KEY_WORKER = SliderKeys.ROLE_AM_PRIORITY_INDEX + 1;

  public static final int KEY_MASTER = SliderKeys.ROLE_AM_PRIORITY_INDEX + 2;

  public static final int KEY_REST_GATEWAY = SliderKeys.ROLE_AM_PRIORITY_INDEX + 3;

  public static final int KEY_THRIFT_GATEWAY = SliderKeys.ROLE_AM_PRIORITY_INDEX + 4;

  public static final int KEY_THRIFT2_GATEWAY = SliderKeys.ROLE_AM_PRIORITY_INDEX + 5;
  /**
   * Initialize role list
   */
  static {
    ROLES.add(new ProviderRole(HBaseKeys.ROLE_WORKER, KEY_WORKER));
    ROLES.add(new ProviderRole(HBaseKeys.ROLE_MASTER, KEY_MASTER));
    ROLES.add(new ProviderRole(HBaseKeys.ROLE_REST_GATEWAY, KEY_REST_GATEWAY));
    ROLES.add(new ProviderRole(HBaseKeys.ROLE_THRIFT_GATEWAY, KEY_THRIFT_GATEWAY));
    ROLES.add(new ProviderRole(HBaseKeys.ROLE_THRIFT_GATEWAY, KEY_THRIFT2_GATEWAY));
  }


  public static List<ProviderRole> getRoles() {
    return ROLES;
  }
}
