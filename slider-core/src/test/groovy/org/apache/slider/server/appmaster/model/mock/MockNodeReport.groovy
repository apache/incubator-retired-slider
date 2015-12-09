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

import groovy.transform.CompileStatic
import org.apache.hadoop.yarn.api.records.NodeId
import org.apache.hadoop.yarn.api.records.NodeReport
import org.apache.hadoop.yarn.api.records.NodeState
import org.apache.hadoop.yarn.api.records.Resource

/**
 * Node report for testing
 */
@CompileStatic
class MockNodeReport extends NodeReport {
  NodeId nodeId;
  NodeState nodeState;
  String httpAddress;
  String rackName;
  Resource used;
  Resource capability;
  int numContainers;
  String healthReport;
  long lastHealthReportTime;
  Set<String> nodeLabels;

  MockNodeReport() {
  }

  /**
   * Create a single instance
   * @param hostname
   * @param nodeState
   * @param label
   */
  MockNodeReport(String hostname, NodeState nodeState, String label ="") {
    nodeId = NodeId.newInstance(hostname, 80)
    Integer.valueOf(hostname, 16)
    this.nodeState = nodeState
    this.httpAddress = "http$hostname:80"
    this.nodeLabels = new HashSet<>()
    nodeLabels.add(label)
  }

  /**
   * Create a list of instances -one for each hostname
   * @param hostnames hosts
   * @param nodeState state of all of them
   * @param label label for all of them
   * @return
   */
  static List<MockNodeReport> createInstances(
      List<String> hostnames,
      NodeState nodeState = NodeState.RUNNING,
      String label = "") {
    hostnames.collect { String  name ->
      new MockNodeReport(name, nodeState, label)}
  }
}
