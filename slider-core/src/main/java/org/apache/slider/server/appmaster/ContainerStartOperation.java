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

package org.apache.slider.server.appmaster;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.slider.server.appmaster.state.RoleInstance;

/**
 * Callback for container start requests
 */
public interface ContainerStartOperation {
  /**
   * Add a node to the list of starting
   * nodes then trigger the NM start operation with the given
   * launch context
   * @param container container
   * @param ctx context
   * @param instance node details
   */
  void startContainer(Container container,
                      ContainerLaunchContext ctx,
                      RoleInstance instance) ;
}
