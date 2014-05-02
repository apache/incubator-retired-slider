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

  /**
   * Initialize role list
   */
  static {
    ROLES.add(new ProviderRole(HBaseKeys.ROLE_WORKER, KEY_WORKER));
    // Master doesn't need data locality
    ROLES.add(new ProviderRole(HBaseKeys.ROLE_MASTER, KEY_MASTER,PlacementPolicy.NO_DATA_LOCALITY));
  }


  public static List<ProviderRole> getRoles() {
    return ROLES;
  }
}
