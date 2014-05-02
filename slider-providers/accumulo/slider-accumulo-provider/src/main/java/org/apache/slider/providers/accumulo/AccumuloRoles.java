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

package org.apache.slider.providers.accumulo;

import org.apache.slider.common.SliderKeys;
import org.apache.slider.providers.ProviderRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.slider.providers.accumulo.AccumuloKeys.ROLE_GARBAGE_COLLECTOR;
import static org.apache.slider.providers.accumulo.AccumuloKeys.ROLE_MASTER;
import static org.apache.slider.providers.accumulo.AccumuloKeys.ROLE_MONITOR;
import static org.apache.slider.providers.accumulo.AccumuloKeys.ROLE_TABLET;
import static org.apache.slider.providers.accumulo.AccumuloKeys.ROLE_TRACER;

public class AccumuloRoles  {
  protected static final Logger log =
    LoggerFactory.getLogger(AccumuloRoles.class);
    
  /**
   * List of roles
   */
  public static final List<ProviderRole> ROLES =
    new ArrayList<ProviderRole>();

  private static int BASE;

  /**
   * Initialize role list
   */
  static {
    BASE = SliderKeys.ROLE_AM_PRIORITY_INDEX;
    AccumuloRoles.ROLES.add(new ProviderRole(ROLE_MASTER, BASE + 1));
    AccumuloRoles.ROLES.add(new ProviderRole(ROLE_TABLET, BASE + 2));
    AccumuloRoles.ROLES.add(new ProviderRole(ROLE_GARBAGE_COLLECTOR, BASE + 3));
    AccumuloRoles.ROLES.add(new ProviderRole(ROLE_MONITOR, BASE + 4));
    AccumuloRoles.ROLES.add(new ProviderRole(ROLE_TRACER, BASE + 5));
  }


  /**
   * Convert a Slider role into the service/classname passed down 
   * to accumulo, (and implicitly , item to grep and kill when force
   * killing services at the end of a test run)
   * @param role role being instantiated
   * @return first argument to Accumulo 
   */
  public static String serviceForRole(String role) {
    for (ProviderRole providerRole : ROLES) {
      if (providerRole.name.equals(role)) {
        return role;
      }
    }
    //unknown role
    log.warn("unknown accumulo role {}", role);
    return role;
  }
  
  public static List<String> roleList() {
    List<String> l = new ArrayList<String>(ROLES.size());
    for (ProviderRole providerRole : ROLES) {
      l.add(providerRole.name);
    }
    return l;
  }
  
  public static List<String> serviceList() {
    List<String> l = new ArrayList<String>(ROLES.size());
    for (ProviderRole providerRole : ROLES) {
      l.add(serviceForRole(providerRole.name));
    }
    return l;
  }
  
  
  
}
