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

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.slider.core.exceptions.SliderException;

/** Operations available to a provider from AppMaster */
public interface AMViewForProviders {

  /**
   * Outcomes from container loss
   */
  enum ContainerLossReportOutcomes {
    /**
     * The container doesn't exist...either it wasn't in use or it
     * has been released
     */
    CONTAINER_NOT_IN_USE,

    /**
     * The container is known about and a review has been initated
     */
    CONTAINER_LOST_REVIEW_INITIATED,
  }
  
  /** Provider can ask AppMaster to release a specific container */
  ContainerLossReportOutcomes providerLostContainer(ContainerId containerId) throws SliderException;
  
}
