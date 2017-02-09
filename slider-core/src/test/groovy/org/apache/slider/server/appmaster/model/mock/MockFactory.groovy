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

import com.google.common.collect.Maps
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId
import org.apache.hadoop.yarn.api.records.ApplicationId
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.api.records.ContainerState
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.hadoop.yarn.api.records.NodeId
import org.apache.hadoop.yarn.api.records.NodeReport
import org.apache.hadoop.yarn.api.records.NodeState
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.api.records.impl.pb.NodeReportPBImpl
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.slider.api.ClusterDescription
import org.apache.slider.api.ResourceKeys
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.providers.PlacementPolicy
import org.apache.slider.providers.ProviderRole

/**
 * Factory for creating things
 */
//@CompileStatic
@Slf4j
class MockFactory implements MockRoles {

  public static final NODE_FAILURE_THRESHOLD = 2;

  public static MockFactory instance = new MockFactory();

  /*
  Ignore any IDE hints about needless references to the ROLE values; groovyc fails without them.
   */

  /**
   * basic role
   */
  public static final ProviderRole PROVIDER_ROLE0 = new ProviderRole(
      MockRoles.ROLE0,
      0,
      PlacementPolicy.DEFAULT,
      NODE_FAILURE_THRESHOLD,
      1,
      ResourceKeys.DEF_YARN_LABEL_EXPRESSION)
  /**
   * role 1 is strict. timeout should be irrelevant; same as failures
   */
  public static final ProviderRole PROVIDER_ROLE1 = new ProviderRole(
      MockRoles.ROLE1,
      1,
      PlacementPolicy.STRICT,
      NODE_FAILURE_THRESHOLD,
      1,
      ResourceKeys.DEF_YARN_LABEL_EXPRESSION)

  /**
   * role 2: longer delay
   */
  public static final ProviderRole PROVIDER_ROLE2 = new ProviderRole(
      MockRoles.ROLE2,
      2,
      PlacementPolicy.ANYWHERE,
      NODE_FAILURE_THRESHOLD,
      2,
      ResourceKeys.DEF_YARN_LABEL_EXPRESSION)

  /**
   * Patch up a "role2" role to have anti-affinity set
   */
  public static final ProviderRole AAROLE_2 = new ProviderRole(
      MockRoles.ROLE2,
      2,
      PlacementPolicy.ANTI_AFFINITY_REQUIRED,
      NODE_FAILURE_THRESHOLD,
      2,
      null)

  /**
   * Patch up a "role1" role to have anti-affinity set and GPI as the label
   */
  public static final ProviderRole AAROLE_1_GPU = new ProviderRole(
      MockRoles.ROLE1,
      1,
      PlacementPolicy.ANTI_AFFINITY_REQUIRED,
      NODE_FAILURE_THRESHOLD,
      1,
      MockRoles.LABEL_GPU)

  int appIdCount;
  int attemptIdCount;
  int containerIdCount;

  ApplicationId appId = newAppId()
  ApplicationAttemptId attemptId = newApplicationAttemptId(appId)

  /**
   * List of roles
   */
  public static final List<ProviderRole> ROLES = [
      PROVIDER_ROLE0,
      PROVIDER_ROLE1,
      PROVIDER_ROLE2,
  ]

  public static final int ROLE_COUNT = ROLES.size();

  MockContainerId newContainerId() {
    newContainerId(attemptId)
  }

  MockContainerId newContainerId(ApplicationAttemptId attemptId) {
    MockContainerId cid = new MockContainerId()
    cid.containerId = containerIdCount++
    cid.applicationAttemptId = attemptId;
    return cid;
  }

  MockApplicationAttemptId newApplicationAttemptId(ApplicationId appId) {
    MockApplicationAttemptId id = new MockApplicationAttemptId()
    id.attemptId = attemptIdCount++;
    id.applicationId = appId
    return id;
  }

  MockApplicationId newAppId() {
    MockApplicationId id = new MockApplicationId()
    id.setId(appIdCount++);
    return id;
  }

  MockNodeId newNodeId(String host = null) {
    new MockNodeId(host: host)
  }

  MockContainer newContainer(ContainerId cid) {
    MockContainer c = new MockContainer()
    c.id = cid
    return c
  }

  MockContainer newContainer() {
    newContainer(newContainerId())
  }

  MockContainer newContainer(NodeId nodeId, Priority priority) {
    def container = newContainer(newContainerId())
    container.nodeId = nodeId
    container.priority = priority
    container
  }

  /**
   * Build a new container  using the request to suppy priority and resource
   * @param req request
   * @param host hostname to assign to
   * @return the container
   */
  MockContainer newContainer(AMRMClient.ContainerRequest req, String host) {
    MockContainer container = newContainer(newContainerId())
    container.resource = req.capability
    container.priority = req.priority
    container.nodeId = new MockNodeId(host)
    return container
  }

  /**
   * Create a cluster spec with the given desired role counts
   * @param r1
   * @param r2
   * @param r3
   * @return
   */
  ClusterDescription newClusterSpec(int r1, int r2, int r3) {
    ClusterDescription cd = new ClusterDescription()
    cd.roles = newComponentsSection(r1, r2, r3)

    return cd
  }

  public HashMap<String, LinkedHashMap<String, String>> newComponentsSection(
      int r1,
      int r2,
      int r3) {
    return Maps.newHashMap([
        (ROLE0): roleMap(r1),
        (ROLE1): roleMap(r2),
        (ROLE2): roleMap(r3),
    ])
  }

  /**
   * Create a cluster spec with the given desired role counts
   * @param r1
   * @param r2
   * @param r3
   * @return
   */
  ConfTree newConfTree(int r1, int r2, int r3) {
    ConfTree cd = new ConfTree()

    cd.components = newComponentsSection(r1, r2, r3)

    return cd
  }

  /**
   * Create a new instance with the given components definined in the
   * resources section
   * @param r1
   * @param r2
   * @param r3
   * @return
   */
  AggregateConf newInstanceDefinition(int r1, int r2, int r3) {
    AggregateConf instance = new AggregateConf()
    instance.setResources(newConfTree(r1, r2, r3))
    return instance
  }

  def roleMap(int count) {
    return [
        (ResourceKeys.COMPONENT_INSTANCES): count.toString(),
    ]
  }

  MockResource newResource(int memory = 0, int vcores = 0) {
    return new MockResource(memory, vcores)
  }

  ContainerStatus newContainerStatus() {
    return newContainerStatus(null, null, "", 0)
  }

  ContainerStatus newContainerStatus(ContainerId containerId,
    ContainerState containerState, String diagnostics, int exitStatus) {
    ContainerStatus.newInstance(containerId, containerState, diagnostics, exitStatus)
  }

  /**
   * Create a single instance
   * @param hostname
   * @param nodeState
   * @param label
   */
  NodeReport newNodeReport(String hostname, NodeState nodeState, String label = "") {
    def nodeId = NodeId.newInstance(hostname, 80)
    Integer.valueOf(hostname, 16)
    newNodeReport(hostname, nodeId, nodeState, label)
  }

  NodeReport newNodeReport(
    String hostname,
    NodeId nodeId,
    NodeState nodeState,
    String label) {
    def report = new NodeReportPBImpl();
    def nodeLabels = new HashSet<>()
    nodeLabels.add(label)
    report.nodeId = nodeId
    report.nodeLabels = nodeLabels
    report.nodeState = nodeState
    report.httpAddress = "http$hostname:80"
    report
  }

  /**
   * Create a list of instances -one for each hostname
   * @param hostnames hosts
   * @param nodeState state of all of them
   * @param label label for all of them
   * @return
   */
  List<NodeReport> createNodeReports(
    List<String> hostnames,
    NodeState nodeState = NodeState.RUNNING,
    String label = "") {
    hostnames.collect { String name ->
      newNodeReport(name, nodeState, label)
    }
  }

}
