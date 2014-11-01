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

package org.apache.slider.server.appmaster.model.mock

import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.api.records.NodeId
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.api.records.Token

class MockContainer extends Container {
  
  ContainerId id;
  NodeId nodeId
  String nodeHttpAddress;
  Resource resource
  Priority priority;
  Token containerToken

  @Override
  public int compareTo(Container other) {
    if (this.getId().compareTo(other.getId()) == 0) {
      if (this.getNodeId().compareTo(other.getNodeId()) == 0) {
        return this.getResource().compareTo(other.getResource());
      } else {
        return this.getNodeId().compareTo(other.getNodeId());
      }
    } else {
      return this.getId().compareTo(other.getId());
    }
  }

  @Override
  public String toString() {
    return "MockContainer{ id=$id" +
           ", nodeHttpAddress='$nodeHttpAddress'," +
           " priority=$priority }"
  }
}
