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

package org.apache.slider.server.appmaster.state;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.util.Records;

/**
 * Class containing the logic to build/split container priorities into the
 * different fields used by Slider
 *
 * The original design here had a requestID merged with the role, to
 * track outstanding requests. However, this isn't possible, so
 * the request ID has been dropped. A "location specified" flag was
 * added to indicate whether or not the request was for a specific location
 * -though this is currently unused.
 * 
 * The methods are effectively surplus -but retained to preserve the
 * option of changing behavior in future
 */
public final class ContainerPriority {

  public static int buildPriority(int role,
                                  boolean locationSpecified) {
    return (role)  ;
  }


  public static Priority createPriority(int role,
                                        boolean locationSpecified) {
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(ContainerPriority.buildPriority(role,
                                                    locationSpecified));
    return pri;
  }
  
  
  public static int extractRole(int priority) {
    return priority ;
  }

  /**
   * Map from a container to a role key by way of its priority
   * @param container container
   * @return role key
   */
  public static int extractRole(Container container) {
    Priority priority = container.getPriority();
    assert priority != null;
    return extractRole(priority.getPriority());
  }
  
  /**
   * Map from a container to a role key by way of its priority
   * @param container container
   * @return role key
   */
  public static int extractRole(Priority priorityRecord) {
    return extractRole(priorityRecord.getPriority());
  }

}
