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

package org.apache.slider.api;

/**
 * These are the keys valid in resource options
 */
public interface ResourceKeys {


  /**
   * #of instances of a component
   *
  */
  String COMPONENT_INSTANCES = "component.instances";

  /**
   *  Amount of memory to ask YARN for in MB.
   *  <i>Important:</i> this may be a hard limit on the
   *  amount of RAM that the service can use
   *  {@value}
   */
  String YARN_MEMORY = "yarn.memory";
  
  /** {@value} */
  int DEF_YARN_MEMORY = 256;
  
  /**
   * Number of cores/virtual cores to ask YARN for
   *  {@value}
   */
  String YARN_CORES = "yarn.vcores";
  
  /** {@value} */
  int DEF_YARN_CORES = 1;
  
  /**
   * Constant to indicate that the requirements of a YARN resource limit
   * (cores, memory, ...) should be set to the maximum allowed by
   * the queue into which the YARN container requests are placed.
   */
  String YARN_RESOURCE_MAX = "max";
  
  /**
   * Mandatory property for all roles
   * 1. this must be defined.
   * 2. this must be >= 1
   * 3. this must not match any other role priority in the cluster.
   */
  String COMPONENT_PRIORITY = "role.priority";
  
  /**
   * placement policy
   */
  String COMPONENT_PLACEMENT_POLICY = "component.placement.policy";
}
