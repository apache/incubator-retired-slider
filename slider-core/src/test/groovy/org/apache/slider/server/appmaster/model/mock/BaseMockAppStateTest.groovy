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
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem as HadoopFS
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.Container
import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.api.records.ContainerState
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.tools.SliderFileSystem
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.server.appmaster.operations.AbstractRMOperation
import org.apache.slider.server.appmaster.state.AppState
import org.apache.slider.server.appmaster.state.ContainerAssignment
import org.apache.slider.server.appmaster.state.ContainerOutcome
import org.apache.slider.server.appmaster.state.NodeEntry
import org.apache.slider.server.appmaster.state.NodeInstance
import org.apache.slider.server.appmaster.state.RoleInstance
import org.apache.slider.server.appmaster.state.RoleStatus
import org.apache.slider.server.appmaster.state.SimpleReleaseSelector
import org.apache.slider.test.SliderTestBase
import org.junit.Before

@CompileStatic
@Slf4j
abstract class BaseMockAppStateTest extends SliderTestBase implements MockRoles {
  public static final int RM_MAX_RAM = 4096
  public static final int RM_MAX_CORES = 64
  MockFactory factory = new MockFactory()
  MockAppState appState
  MockYarnEngine engine
  protected HadoopFS fs
  protected SliderFileSystem sliderFileSystem
  protected File historyWorkDir
  protected Path historyPath;
  protected MockApplicationId applicationId;
  protected MockApplicationAttemptId applicationAttemptId;

  @Override
  void setup() {
    super.setup()
    YarnConfiguration conf = SliderUtils.createConfiguration()
    fs = HadoopFS.get(new URI("file:///"), conf)
    sliderFileSystem = new SliderFileSystem(fs, conf)
    engine = createYarnEngine()
  }

  /**
   * Override point: called in setup() to create the YARN engine; can
   * be changed for different sizes and options
   * @return
   */
  public MockYarnEngine createYarnEngine() {
    return new MockYarnEngine(64, 1)
  }

  @Before
  void initApp(){

    String historyDirName = testName;


    YarnConfiguration conf = SliderUtils.createConfiguration()
    applicationId = new MockApplicationId(id: 1, clusterTimestamp: 0)
    applicationAttemptId = new MockApplicationAttemptId(
        applicationId: applicationId,
        attemptId: 1)

    fs = HadoopFS.get(new URI("file:///"), conf)
    historyWorkDir = new File("target/history", historyDirName)
    historyPath = new Path(historyWorkDir.toURI())
    fs.delete(historyPath, true)
    appState = new MockAppState()
    appState.setContainerLimits(RM_MAX_RAM, RM_MAX_CORES)
    appState.buildInstance(
        buildInstanceDefinition(),
        new Configuration(),
        new Configuration(false),
        factory.ROLES,
        fs,
        historyPath,
        null, null,
        new SimpleReleaseSelector())
  }

  /**
   * Override point, define the instance definition
   * @return
   */
  public AggregateConf buildInstanceDefinition() {
    factory.newInstanceDefinition(0, 0, 0)
  }

  /**
   * Get the test name ... defaults to method name
   * @return
   */
  String getTestName() {
    methodName.methodName;
  }

  public RoleStatus getRole0Status() {
    return appState.lookupRoleStatus(ROLE0)
  }

  public RoleStatus getRole1Status() {
    return appState.lookupRoleStatus(ROLE1)
  }

  public RoleStatus getRole2Status() {
    return appState.lookupRoleStatus(ROLE2)
  }

  /**
   * Build a role instance from a container assignment
   * @param assigned
   * @return the instance
   */
  RoleInstance roleInstance(ContainerAssignment assigned) {
    Container target = assigned.container
    RoleInstance ri = new RoleInstance(target)
    ri.roleId = assigned.role.priority
    ri.role = assigned.role.name
    return ri
  }


  public NodeInstance nodeInstance(long age, int live0, int live1=0, int live2=0) {
    NodeInstance ni = new NodeInstance("age${age}-[${live0},${live1},$live2]",
                                       MockFactory.ROLE_COUNT)
    ni.getOrCreate(0).lastUsed = age
    ni.getOrCreate(0).live = live0;
    if (live1 > 0) {
      ni.getOrCreate(1).live = live1;
    }
    if (live2 > 0) {
      ni.getOrCreate(2).live = live2;
    }
    return ni
  }

  /**
   * Create a container status event
   * @param c container
   * @return a status
   */
  ContainerStatus containerStatus(Container c) {
    return containerStatus(c.id)
  }

  /**
   * Create a container status instance for the given ID, declaring
   * that it was shut down by the application itself
   * @param cid container Id
   * @return the instance
   */
  public ContainerStatus containerStatus(ContainerId cid) {
    ContainerStatus status = containerStatus(cid,
        LauncherExitCodes.EXIT_CLIENT_INITIATED_SHUTDOWN)
    return status
  }

  public ContainerStatus containerStatus(ContainerId cid, int exitCode) {
    ContainerStatus status = ContainerStatus.newInstance(
        cid,
        ContainerState.COMPLETE,
        "",
        exitCode)
    return status
  }

  /**
   * Create nodes and bring them to the started state
   * @return a list of roles
   */
  protected List<RoleInstance> createAndStartNodes() {
    return createStartAndStopNodes([])
  }


  /**
   * Create, Start and stop nodes
   * @param completionResults List filled in with the status on all completed nodes
   * @return the nodes
   */
  public List<RoleInstance> createStartAndStopNodes(
      List<AppState.NodeCompletionResult> completionResults) {
    List<ContainerId> released = []
    List<RoleInstance> instances = createAndSubmitNodes(released)
    processSubmissionOperations(instances, completionResults, released)
    return instances
  }

  /**
   * Process the start/stop operations from 
   * @param instances
   * @param completionResults
   * @param released
   */
  public void processSubmissionOperations(
      List<RoleInstance> instances,
      List<AppState.NodeCompletionResult> completionResults,
      List<ContainerId> released) {
    for (RoleInstance instance : instances) {
      log.debug("Started ${instance.role} on ${instance.id} ")
      assert appState.onNodeManagerContainerStarted(instance.containerId)
    }
    releaseContainers(completionResults,
        released,
        ContainerState.COMPLETE,
        "released",
        0
    )
  }

  /**
   * Release a list of containers, updating the completion results
   * @param completionResults
   * @param containerIds
   * @param containerState
   * @param exitText
   * @param containerExitCode
   * @return
   */
  public def releaseContainers(
      List<AppState.NodeCompletionResult> completionResults,
      List<ContainerId> containerIds,
      ContainerState containerState,
      String exitText,
      int containerExitCode) {
    containerIds.each { ContainerId id ->
      ContainerStatus status = ContainerStatus.newInstance(id,
          containerState,
          exitText,
          containerExitCode)
      completionResults << appState.onCompletedNode(status)

    }
  }

  /**
   * Create nodes and submit them
   * @return a list of roles
   */
  public List<RoleInstance> createAndSubmitNodes() {
    return createAndSubmitNodes([])
  }

  /**
   * Create nodes and submit them
   * @param released a list that is built up of all released nodes
   * @return a list of roles allocated
   */
  public List<RoleInstance> createAndSubmitNodes(
      List<ContainerId> released) {
    List<AbstractRMOperation> ops = appState.reviewRequestAndReleaseNodes()
    return submitOperations(ops, released)
  }

  /**
   * Process the RM operations and send <code>onContainersAllocated</code>
   * events to the app state
   * @param ops
   * @param released
   * @return
   */
  public List<RoleInstance> submitOperations(
      List<AbstractRMOperation> ops,
      List<ContainerId> released) {
    List<Container> allocatedContainers = engine.execute(ops, released)
    List<ContainerAssignment> assignments = [];
    List<AbstractRMOperation> operations = []
    appState.onContainersAllocated(allocatedContainers, assignments, operations)
    List<RoleInstance> instances = []
    for (ContainerAssignment assigned : assignments) {
      Container container = assigned.container
      RoleInstance ri = roleInstance(assigned)
      instances << ri
      //tell the app it arrived
      log.debug("Start submitted ${ri.role} on ${container.id} ")
      appState.containerStartSubmitted(container, ri);
    }
    return instances
  }

  /**
   * Add the AM to the app state
   */
  protected void addAppMastertoAppState() {
    appState.buildAppMasterNode(
        new MockContainerId(applicationAttemptId, 999999L),
        "appmaster",
        0,
        null)
  }
  
  /**
   * Extract the list of container IDs from the list of role instances
   * @param instances instance list
   * @param role role to look up
   * @return the list of CIDs
   */
  List<ContainerId> extractContainerIds(
      List<RoleInstance> instances,
      int role) {
    List<ContainerId> cids = []
    instances.each { RoleInstance instance ->
      if (instance.roleId == role) {
        cids << instance.id
      }
    }
    return cids
  }

  /**
   * Record a node as failing
   * @param node
   * @param id
   * @param count
   * @return the entry
   */
  public NodeEntry recordAsFailed(NodeInstance node, int id, int count) {
    def entry = node.getOrCreate(id)
    1.upto(count) {
      entry.containerCompleted(
          false,
          ContainerOutcome.Failed)
    }
    entry
  }
}
