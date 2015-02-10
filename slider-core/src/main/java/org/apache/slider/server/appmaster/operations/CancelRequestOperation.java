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

package org.apache.slider.server.appmaster.operations;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.slider.server.appmaster.state.ContainerPriority;

/**
 * Cancel a container request
 */
public class CancelRequestOperation extends AbstractRMOperation {

  private final Priority priority1;
  private final Priority priority2;
  private final int count;

  public CancelRequestOperation(Priority priority1, Priority priority2, int count) {
    this.priority1 = priority1;
    this.priority2 = priority2;
    this.count = count;
  }

  @Override
  public void execute(RMOperationHandlerActions handler) {
    handler.cancelContainerRequests(priority1, priority2, count);
  }

  @Override
  public String toString() {
    return "release " + count
           + " requests for " + ContainerPriority.toString(priority1)
           + " and " + ContainerPriority.toString(priority2);
  }

  /**
   * Get the number to release
   * @return the number of containers to release
   */
  public int getCount() {
    return count;
  }
}
