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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterDescriptionKeys;
import org.apache.slider.api.ClusterDescriptionOperations;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.api.proto.Messages;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.tools.ConfigHelper;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTree;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.exceptions.SliderInternalStateException;
import org.apache.slider.core.exceptions.TriggerClusterTeardownException;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;
import org.apache.slider.providers.ProviderRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.slider.api.ResourceKeys.DEF_YARN_CORES;
import static org.apache.slider.api.ResourceKeys.DEF_YARN_MEMORY;
import static org.apache.slider.api.ResourceKeys.YARN_CORES;
import static org.apache.slider.api.ResourceKeys.YARN_MEMORY;
import static org.apache.slider.api.RoleKeys.ROLE_FAILED_INSTANCES;
import static org.apache.slider.api.RoleKeys.ROLE_FAILED_STARTING_INSTANCES;
import static org.apache.slider.api.RoleKeys.ROLE_RELEASING_INSTANCES;
import static org.apache.slider.api.RoleKeys.ROLE_REQUESTED_INSTANCES;


/**
 * The model of all the ongoing state of a Slider AM.
 *
 * concurrency rules: any method which begins with <i>build</i>
 * is not synchronized and intended to be used during
 * initialization.
 */
public class AppState implements StateAccessForProviders {
  protected static final Logger log =
    LoggerFactory.getLogger(AppState.class);
  
  private final AbstractRecordFactory recordFactory;

  /**
   * Flag set to indicate the application is live -this only happens
   * after the buildInstance operation
   */
  boolean applicationLive = false;

  /**
   * The definition of the instance. Flexing updates the resources section
   This is used as a synchronization point on activities that update
   the CD, and also to update some of the structures that
   feed in to the CD
   */
  private AggregateConf instanceDefinition;
  
  private long snapshotTime;
  private AggregateConf instanceDefinitionSnapshot;

  /**
   * snapshot of resources as of last update time
   */
  private ConfTreeOperations resourcesSnapshot;
  private ConfTreeOperations appConfSnapshot;
  private ConfTreeOperations internalsSnapshot;
  
  /**
   * This is the status, the live model
   */
  private ClusterDescription clusterStatus = new ClusterDescription();

  /**
   * Metadata provided by the AM for use in filling in status requests
   */
  private Map<String, String> applicationInfo;
  
  /**
   * Client properties created via the provider -static for the life
   * of the application
   */
  private Map<String, String> clientProperties = new HashMap<String, String>();

  /**
   The cluster description published to callers
   This is used as a synchronization point on activities that update
   the CD, and also to update some of the structures that
   feed in to the CD
   */
  private ClusterDescription clusterSpec = new ClusterDescription();

  private final PublishedConfigSet
      publishedConfigurations = new PublishedConfigSet();


  private final Map<Integer, RoleStatus> roleStatusMap =
    new ConcurrentHashMap<Integer, RoleStatus>();

  private final Map<String, ProviderRole> roles =
    new ConcurrentHashMap<String, ProviderRole>();

  /**
   * The master node.
   */
  private RoleInstance appMasterNode;

  /**
   * Hash map of the containers we have. This includes things that have
   * been allocated but are not live; it is a superset of the live list
   */
  private final ConcurrentMap<ContainerId, RoleInstance> activeContainers =
    new ConcurrentHashMap<ContainerId, RoleInstance>();

  /**
   * Hash map of the containers we have released, but we
   * are still awaiting acknowledgements on. Any failure of these
   * containers is treated as a successful outcome
   */
  private final ConcurrentMap<ContainerId, Container> containersBeingReleased =
    new ConcurrentHashMap<ContainerId, Container>();
  
  /**
   * Counter for completed containers ( complete denotes successful or failed )
   */
  private final AtomicInteger completedContainerCount = new AtomicInteger();

  /**
   *   Count of failed containers

   */
  private final AtomicInteger failedContainerCount = new AtomicInteger();

  /**
   * # of started containers
   */
  private final AtomicInteger startedContainers = new AtomicInteger();

  /**
   * # of containers that failed to start 
   */
  private final AtomicInteger startFailedContainers = new AtomicInteger();

  /**
   * Track the number of surplus containers received and discarded
   */
  private final AtomicInteger surplusContainers = new AtomicInteger();


  /**
   * Map of requested nodes. This records the command used to start it,
   * resources, etc. When container started callback is received,
   * the node is promoted from here to the containerMap
   */
  private final Map<ContainerId, RoleInstance> startingNodes =
    new ConcurrentHashMap<ContainerId, RoleInstance>();

  /**
   * List of completed nodes. This isn't kept in the CD as it gets too
   * big for the RPC responses. Indeed, we should think about how deep to get this
   */
  private final Map<ContainerId, RoleInstance> completedNodes
    = new ConcurrentHashMap<ContainerId, RoleInstance>();

  /**
   * Nodes that failed to start.
   * Again, kept out of the CD
   */
  private final Map<ContainerId, RoleInstance> failedNodes =
    new ConcurrentHashMap<ContainerId, RoleInstance>();

  /**
   * Nodes that came assigned to a role above that
   * which were asked for -this appears to happen
   */
  private final Set<ContainerId> surplusNodes = new HashSet<ContainerId>();

  /**
   * Map of containerID -> cluster nodes, for status reports.
   * Access to this should be synchronized on the clusterDescription
   */
  private final Map<ContainerId, RoleInstance> liveNodes =
    new ConcurrentHashMap<ContainerId, RoleInstance>();
  private final AtomicInteger completionOfNodeNotInLiveListEvent =
    new AtomicInteger();
  private final AtomicInteger completionOfUnknownContainerEvent =
    new AtomicInteger();


  /**
   * Record of the max no. of cores allowed in this cluster
   */
  private int containerMaxCores;

  /**
   * limit container memory
   */
  private int containerMaxMemory;
  
  private RoleHistory roleHistory;
  private Configuration publishedProviderConf;
  private long startTimeThreshold;
  
  private int failureThreshold = 10;

  public AppState(AbstractRecordFactory recordFactory) {
    this.recordFactory = recordFactory;
  }

  public int getFailedCountainerCount() {
    return failedContainerCount.get();
  }

  /**
   * Increment the count and return the new value
   * @return the latest failed container count
   */
  public int incFailedCountainerCount() {
    return failedContainerCount.incrementAndGet();
  }

  public int getStartFailedCountainerCount() {
    return startFailedContainers.get();
  }

  /**
   * Increment the count and return the new value
   * @return the latest failed container count
   */
  public int incStartedCountainerCount() {
    return startedContainers.incrementAndGet();
  }

  public int getStartedCountainerCount() {
    return startedContainers.get();
  }

  /**
   * Increment the count and return the new value
   * @return the latest failed container count
   */
  public int incStartFailedCountainerCount() {
    return startFailedContainers.incrementAndGet();
  }

  
  public AtomicInteger getStartFailedContainers() {
    return startFailedContainers;
  }

  public AtomicInteger getCompletionOfNodeNotInLiveListEvent() {
    return completionOfNodeNotInLiveListEvent;
  }

  public AtomicInteger getCompletionOfUnknownContainerEvent() {
    return completionOfUnknownContainerEvent;
  }

  @Override
  public Map<Integer, RoleStatus> getRoleStatusMap() {
    return roleStatusMap;
  }
  
  protected Map<String, ProviderRole> getRoleMap() {
    return roles;
  }

  private Map<ContainerId, RoleInstance> getStartingNodes() {
    return startingNodes;
  }

  private Map<ContainerId, RoleInstance> getCompletedNodes() {
    return completedNodes;
  }


  @Override
  public PublishedConfigSet getPublishedConfigurations() {
    return publishedConfigurations;
  }  
  

  @Override
  public Map<ContainerId, RoleInstance> getFailedNodes() {
    return failedNodes;
  }

  @Override
  public Map<ContainerId, RoleInstance> getLiveNodes() {
    return liveNodes;
  }

  /**
   * Get the desired cluster state
   * @return the specification of the cluter
   */
  public ClusterDescription getClusterSpec() {
    return clusterSpec;
  }

  @Override
  public ClusterDescription getClusterStatus() {
    return clusterStatus;
  }

  @VisibleForTesting
  protected void setClusterStatus(ClusterDescription clusterDesc) {
    this.clusterStatus = clusterDesc;
  }

  /**
   * Set the instance definition -this also builds the (now obsolete)
   * cluster specification from it.
   * 
   * Important: this is for early binding and must not be used after the build
   * operation is complete. 
   * @param definition
   * @throws BadConfigException
   */
  public synchronized void updateInstanceDefinition(AggregateConf definition) throws
                                                                              BadConfigException,
                                                                              IOException {
    this.instanceDefinition = definition;
    onInstanceDefinitionUpdated();
  }

  public synchronized AggregateConf getInstanceDefinition() {
    return instanceDefinition;
  }

  /**
   * Get the role history of the application
   * @return the role history
   */
  @VisibleForTesting
  public RoleHistory getRoleHistory() {
    return roleHistory;
  }

  /**
   * Get the path used for history files
   * @return the directory used for history files
   */
  @VisibleForTesting
  public Path getHistoryPath() {
    return roleHistory.getHistoryPath();
  }

  /**
   * Set the container limits -the max that can be asked for,
   * which are used when the "max" values are requested
   * @param maxMemory maximum memory
   * @param maxCores maximum cores
   */
  public void setContainerLimits(int maxMemory, int maxCores) {
    containerMaxCores = maxCores;
    containerMaxMemory = maxMemory;
  }

  @Override
  public ConfTreeOperations getResourcesSnapshot() {
    return resourcesSnapshot;
  }

  @Override
  public ConfTreeOperations getAppConfSnapshot() {
    return appConfSnapshot;
  }

  @Override
  public ConfTreeOperations getInternalsSnapshot() {
    return internalsSnapshot;
  }

  @Override
  public boolean isApplicationLive() {
    return applicationLive;
  }


  @Override
  public long getSnapshotTime() {
    return snapshotTime;
  }

  @Override
  public AggregateConf getInstanceDefinitionSnapshot() {
    return instanceDefinitionSnapshot;
  }

  /**
   * Build up the application state
   * @param instanceDefinition definition of the applicatin instance
   * @param publishedProviderConf any configuration info to be published by a provider
   * @param providerRoles roles offered by a provider
   * @param fs filesystem
   * @param historyDir directory containing history files
   * @param liveContainers list of live containers supplied on an AM restart
   * @param applicationInfo
   */
  public synchronized void buildInstance(AggregateConf instanceDefinition,
                            Configuration publishedProviderConf,
                            List<ProviderRole> providerRoles,
                            FileSystem fs,
                            Path historyDir,
                            List<Container> liveContainers,
                            Map<String, String> applicationInfo) throws
                                                                 BadClusterStateException,
                                                                 BadConfigException,
                                                                 IOException {
    this.publishedProviderConf = publishedProviderConf;
    this.applicationInfo = applicationInfo != null ? applicationInfo 
                                         : new HashMap<String, String>();

    clientProperties = new HashMap<String, String>();


    Set<String> confKeys = ConfigHelper.sortedConfigKeys(publishedProviderConf);

//     Add the -site configuration properties
    for (String key : confKeys) {
      String val = publishedProviderConf.get(key);
      clientProperties.put(key, val);
    }
    
    
    // set the cluster specification (once its dependency the client properties
    // is out the way

    updateInstanceDefinition(instanceDefinition);


    //build the initial role list
    for (ProviderRole providerRole : providerRoles) {
      buildRole(providerRole);
    }

    ConfTreeOperations resources =
      instanceDefinition.getResourceOperations();
    
    Set<String> roleNames = resources.getComponentNames();
    for (String name : roleNames) {
      if (!roles.containsKey(name)) {
        // this is a new value
        log.info("Adding new role {}", name);
        MapOperations resComponent = resources.getComponent(name);
        ProviderRole dynamicRole = createDynamicProviderRole(name, resComponent);
        buildRole(dynamicRole);
        providerRoles.add(dynamicRole);
      }
    }
    //then pick up the requirements
    buildRoleRequirementsFromResources();


    //set the livespan
    MapOperations globalInternalOpts =
      instanceDefinition.getInternalOperations().getGlobalOptions();
    startTimeThreshold = globalInternalOpts.getOptionInt(
      OptionKeys.INTERNAL_CONTAINER_FAILURE_SHORTLIFE,
      OptionKeys.DEFAULT_CONTAINER_FAILURE_SHORTLIFE);
    
    failureThreshold = globalInternalOpts.getOptionInt(
      OptionKeys.INTERNAL_CONTAINER_FAILURE_THRESHOLD,
      OptionKeys.DEFAULT_CONTAINER_FAILURE_THRESHOLD);
    initClusterStatus();


    // add the roles
    roleHistory = new RoleHistory(providerRoles);
    roleHistory.onStart(fs, historyDir);
    
    //rebuild any live containers
    rebuildModelFromRestart(liveContainers);
    
    //mark as live
    applicationLive = true;
  }

  public void initClusterStatus() {
    //copy into cluster status. 
    ClusterDescription status = ClusterDescription.copy(clusterSpec);
    status.state = ClusterDescription.STATE_CREATED;
    MapOperations infoOps = new MapOperations("info", status.info);
    infoOps.mergeWithoutOverwrite(applicationInfo);
    SliderUtils.addBuildInfo(infoOps, "status");

    long now = now();
    status.setInfoTime(StatusKeys.INFO_LIVE_TIME_HUMAN,
                              StatusKeys.INFO_LIVE_TIME_MILLIS,
                              now);
    SliderUtils.setInfoTime(infoOps,
        StatusKeys.INFO_LIVE_TIME_HUMAN,
        StatusKeys.INFO_LIVE_TIME_MILLIS,
        now);
    if (0 == status.createTime) {
      status.createTime = now;
      SliderUtils.setInfoTime(infoOps,
          StatusKeys.INFO_CREATE_TIME_HUMAN,
          StatusKeys.INFO_CREATE_TIME_MILLIS,
          now);
    }
    status.state = ClusterDescription.STATE_LIVE;

      //set the app state to this status
    setClusterStatus(status);
  }

  /**
   * Build a dynamic provider role
   * @param name name of role
   * @return a new provider role
   * @throws BadConfigException bad configuration
   */
  public ProviderRole createDynamicProviderRole(String name,
                                                MapOperations component) throws
                                                        BadConfigException {
    String priOpt = component.getMandatoryOption(ResourceKeys.COMPONENT_PRIORITY);
    int pri = SliderUtils.parseAndValidate("value of " + name + " " +
                                           ResourceKeys.COMPONENT_PRIORITY,
        priOpt, 0, 1, -1
    );
    log.info("Role {} assigned priority {}", name, pri);
    String placementOpt = component.getOption(
      ResourceKeys.COMPONENT_PLACEMENT_POLICY, "0");
    int placement = SliderUtils.parseAndValidate("value of " + name + " " +
                                                 ResourceKeys.COMPONENT_PLACEMENT_POLICY,
        placementOpt, 0, 0, -1
    );
    return new ProviderRole(name, pri, placement);
  }


  /**
   * Actions to perform when an instance definition is updated
   * Currently: resolve the configuration
   *  updated the cluster spec derivative
   * @throws BadConfigException
   */
  private synchronized void onInstanceDefinitionUpdated() throws
                                                          BadConfigException,
                                                          IOException {
    instanceDefinition.resolve();

    //note the time 
    snapshotTime = now();
    //snapshot all three sectons
    resourcesSnapshot =
      ConfTreeOperations.fromInstance(instanceDefinition.getResources());
    appConfSnapshot =
      ConfTreeOperations.fromInstance(instanceDefinition.getAppConf());
    internalsSnapshot =
      ConfTreeOperations.fromInstance(instanceDefinition.getInternal());
    //build a new aggregate from the snapshots
    instanceDefinitionSnapshot = new AggregateConf(resourcesSnapshot.confTree,
                                                   appConfSnapshot.confTree,
                                                   internalsSnapshot.confTree);

    clusterSpec =
      ClusterDescriptionOperations.buildFromInstanceDefinition(
        instanceDefinition);
    

//     Add the -site configuration properties
    for (Map.Entry<String, String> prop : clientProperties.entrySet()) {
      clusterSpec.clientProperties.put(prop.getKey(), prop.getValue());
    }
    
  }
  
  /**
   * The resource configuration is updated -review and update state
   * @param resources updated resources specification
   */
  public synchronized void updateResourceDefinitions(ConfTree resources) throws
                                                                         BadConfigException,
                                                                         IOException {
    log.debug("Updating resources to {}", resources);
    
    instanceDefinition.setResources(resources);
    onInstanceDefinitionUpdated();
    
    
    //propagate the role table

    Map<String, Map<String, String>> updated = resources.components;
    getClusterStatus().roles = SliderUtils.deepClone(updated);
    getClusterStatus().updateTime = now();
    buildRoleRequirementsFromResources();
  }

  /**
   * build the role requirements from the cluster specification
   */
  private void buildRoleRequirementsFromResources() throws
                                                      BadConfigException {
    //now update every role's desired count.
    //if there are no instance values, that role count goes to zero

    ConfTreeOperations resources =
      instanceDefinition.getResourceOperations();

    // Add all the existing roles
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      int currentDesired = roleStatus.getDesired();
      String role = roleStatus.getName();
      MapOperations comp =
        resources.getComponent(role);
      int desiredInstanceCount =
        resources.getComponentOptInt(role, ResourceKeys.COMPONENT_INSTANCES, 0);
      if (desiredInstanceCount == 0) {
        log.warn("Role {} has 0 instances specified", role);
      }
      if (currentDesired != desiredInstanceCount) {
        log.info("Role {} flexed from {} to {}", role, currentDesired,
                 desiredInstanceCount);
        roleStatus.setDesired(desiredInstanceCount);
      }
    }
    //now the dynamic ones. Iterate through the the cluster spec and
    //add any role status entries not in the role status
    Set<String> roleNames = resources.getComponentNames();
    for (String name : roleNames) {
      if (!roles.containsKey(name)) {
        // this is a new value
        log.info("Adding new role {}", name);
        ProviderRole dynamicRole = createDynamicProviderRole(name,
                               resources.getComponent(name));
        buildRole(dynamicRole);
        roleHistory.addNewProviderRole(dynamicRole);
      }
    }
  }

  /**
   * Add knowledge of a role.
   * This is a build-time operation that is not synchronized, and
   * should be used while setting up the system state -before servicing
   * requests.
   * @param providerRole role to add
   */
  public void buildRole(ProviderRole providerRole) throws BadConfigException {
    //build role status map
    int priority = providerRole.id;
    if (roleStatusMap.containsKey(priority)) {
      throw new BadConfigException("Duplicate Provider Key: %s and %s",
                                   providerRole,
                                   roleStatusMap.get(priority));
    }
    roleStatusMap.put(priority,
                      new RoleStatus(providerRole));
    roles.put(providerRole.name, providerRole);
  }

  /**
   * build up the special master node, which lives
   * in the live node set but has a lifecycle bonded to the AM
   * @param containerId the AM master
   * @param host
   * @param nodeHttpAddress
   */
  public void buildAppMasterNode(ContainerId containerId,
                                 String host,
                                 int amPort,
                                 String nodeHttpAddress) {
    Container container = new ContainerPBImpl();
    container.setId(containerId);
    NodeId nodeId = NodeId.newInstance(host, amPort);
    container.setNodeId(nodeId);
    container.setNodeHttpAddress(nodeHttpAddress);
    RoleInstance am = new RoleInstance(container);
    am.role = SliderKeys.COMPONENT_AM;
    appMasterNode = am;
    //it is also added to the set of live nodes
    getLiveNodes().put(containerId, am);
  }

  /**
   * Note that the master node has been launched,
   * though it isn't considered live until any forked
   * processes are running. It is NOT registered with
   * the role history -the container is incomplete
   * and it will just cause confusion
   */
  public void noteAMLaunched() {
    getLiveNodes().put(appMasterNode.getContainerId(), appMasterNode);
  }

  /**
   * AM declares ourselves live in the cluster description.
   * This is meant to be triggered from the callback
   * indicating the spawned process is up and running.
   */
  public void noteAMLive() {
    appMasterNode.state = ClusterDescription.STATE_LIVE;
  }

  public RoleInstance getAppMasterNode() {
    return appMasterNode;
  }

  @Override
  public RoleStatus lookupRoleStatus(int key) {
    RoleStatus rs = getRoleStatusMap().get(key);
    if (rs == null) {
      throw new RuntimeException("Cannot find role for role ID " + key);
    }
    return rs;
  }

  @Override
  public RoleStatus lookupRoleStatus(Container c) throws YarnRuntimeException {
    return lookupRoleStatus(ContainerPriority.extractRole(c));
  }


  @Override
  public RoleStatus lookupRoleStatus(String name) throws YarnRuntimeException {
    ProviderRole providerRole = roles.get(name);
    if (providerRole == null) {
      throw new YarnRuntimeException("Unknown role " + name);
    }
    return lookupRoleStatus(providerRole.id);
  }

  @Override
  public synchronized List<RoleInstance> cloneActiveContainerList() {
    Collection<RoleInstance> values = activeContainers.values();
    return new ArrayList<RoleInstance>(values);
  }
  
  @Override
  public int getNumActiveContainers() {
    return activeContainers.size();
  }
  
  @Override
  public RoleInstance getActiveContainer(ContainerId id) {
    return activeContainers.get(id);
  }

  @Override
  public synchronized List<RoleInstance> cloneLiveContainerInfoList() {
    List<RoleInstance> allRoleInstances;
    Collection<RoleInstance> values = getLiveNodes().values();
    allRoleInstances = new ArrayList<RoleInstance>(values);
    return allRoleInstances;
  }


  @Override
  public synchronized RoleInstance getLiveInstanceByContainerID(String containerId)
    throws NoSuchNodeException {
    Collection<RoleInstance> nodes = getLiveNodes().values();
    for (RoleInstance node : nodes) {
      if (containerId.equals(node.id)) {
        return node;
      }
    }
    //at this point: no node
    throw new NoSuchNodeException(containerId);
  }

  @Override
  public synchronized List<RoleInstance> getLiveInstancesByContainerIDs(
    Collection<String> containerIDs) {
    //first, a hashmap of those containerIDs is built up
    Set<String> uuidSet = new HashSet<String>(containerIDs);
    List<RoleInstance> nodes = new ArrayList<RoleInstance>(uuidSet.size());
    Collection<RoleInstance> clusterNodes = getLiveNodes().values();

    for (RoleInstance node : clusterNodes) {
      if (uuidSet.contains(node.id)) {
        nodes.add(node);
      }
    }
    //at this point: a possibly empty list of nodes
    return nodes;
  }

  /**
   * Enum all nodes by role. 
   * @param role role, or "" for all roles
   * @return a list of nodes, may be empty
   */
  public synchronized List<RoleInstance> enumLiveNodesInRole(String role) {
    List<RoleInstance> nodes = new ArrayList<RoleInstance>();
    Collection<RoleInstance> allRoleInstances = getLiveNodes().values();
    for (RoleInstance node : allRoleInstances) {
      if (role.isEmpty() || role.equals(node.role)) {
        nodes.add(node);
      }
    }
    return nodes;
  }


  /**
   * Build an instance map.
   * @return the map of Role name to list of role instances
   */
  private synchronized Map<String, List<String>> createRoleToInstanceMap() {
    Map<String, List<String>> map = new HashMap<String, List<String>>();
    for (RoleInstance node : getLiveNodes().values()) {
      List<String> containers = map.get(node.role);
      if (containers == null) {
        containers = new ArrayList<String>();
        map.put(node.role, containers);
      }
      containers.add(node.id);
    }
    return map;
  }
  /**
   * Build an instance map to send over the wire
   * @return the map of Role name to list of Cluster Nodes, ready
   */
  private synchronized Map<String, Map<String, ClusterNode>> createRoleToClusterNodeMap() {
    Map<String, Map<String, ClusterNode>> map =
      new HashMap<String, Map<String, ClusterNode>>();
    for (RoleInstance node : getLiveNodes().values()) {
      
      Map<String, ClusterNode> containers = map.get(node.role);
      if (containers == null) {
        containers = new HashMap<String, ClusterNode>();
        map.put(node.role, containers);
      }
      Messages.RoleInstanceState pbuf = node.toProtobuf();
      ClusterNode clusterNode = ClusterNode.fromProtobuf(pbuf);
      containers.put(clusterNode.name, clusterNode);
    }
    return map;
  }

  /**
   * Notification called just before the NM is asked to 
   * start a container
   * @param container container to start
   * @param instance clusterNode structure
   */
  public void containerStartSubmitted(Container container,
                                      RoleInstance instance) {
    instance.state = ClusterDescription.STATE_SUBMITTED;
    instance.container = container;
    instance.createTime = now();
    getStartingNodes().put(container.getId(), instance);
    activeContainers.put(container.getId(), instance);
    roleHistory.onContainerStartSubmitted(container, instance);
  }

  /**
   * Note that a container has been submitted for release; update internal state
   * and mark the associated ContainerInfo released field to indicate that
   * while it is still in the active list, it has been queued for release.
   *
   * @param container container
   * @throws SliderInternalStateException if there is no container of that ID
   * on the active list
   */
  public synchronized void containerReleaseSubmitted(Container container)
      throws SliderInternalStateException {
    ContainerId id = container.getId();
    //look up the container
    RoleInstance info = getActiveContainer(id);
    if (info == null) {
      throw new SliderInternalStateException(
        "No active container with ID " + id.toString());
    }
    //verify that it isn't already released
    if (containersBeingReleased.containsKey(id)) {
      throw new SliderInternalStateException(
        "Container %s already queued for release", id);
    }
    info.released = true;
    containersBeingReleased.put(id, info.container);
    RoleStatus role = lookupRoleStatus(info.roleId);
    role.incReleasing();
    roleHistory.onContainerReleaseSubmitted(container);
  }


  /**
   * Set up the resource requirements with all that this role needs, 
   * then create the container request itself.
   * @param role role to ask an instance of
   * @param capability a resource to set up
   * @return the request for a new container
   */
  public AMRMClient.ContainerRequest buildContainerResourceAndRequest(
        RoleStatus role,
        Resource capability) {
    buildResourceRequirements(role, capability);
    //get the role history to select a suitable node, if available
    AMRMClient.ContainerRequest containerRequest =
    createContainerRequest(role, capability);
    return  containerRequest;
  }

  /**
   * Create a container request.
   * Update internal state, such as the role request count
   * This is where role history information will be used for placement decisions -
   * @param role role
   * @param resource requirements
   * @return the container request to submit
   */
  public AMRMClient.ContainerRequest createContainerRequest(RoleStatus role,
                                                            Resource resource) {
    
    
    AMRMClient.ContainerRequest request;
    int key = role.getKey();
    request = roleHistory.requestNode(role, resource);
    role.incRequested();

    return request;
  }


  /**
   * Get the value of a YARN requirement (cores, RAM, etc).
   * These are returned as integers, but there is special handling of the 
   * string {@link ResourceKeys#YARN_RESOURCE_MAX}, which triggers
   * the return of the maximum value.
   * @param name component to get from
   * @param option option name
   * @param defVal default value
   * @param maxVal value to return if the max val is requested
   * @return parsed value
   * @throws NumberFormatException if the role could not be parsed.
   */
  private int getResourceRequirement(ConfTreeOperations resources,
                                     String name,
                                     String option,
                                     int defVal,
                                     int maxVal) {
    
    String val = resources.getComponentOpt(name, option,
                                           Integer.toString(defVal));
    Integer intVal;
    if (ResourceKeys.YARN_RESOURCE_MAX.equals(val)) {
      intVal = maxVal;
    } else {
      intVal = Integer.decode(val);
    }
    return intVal;
  }
  
  /**
   * Build up the resource requirements for this role from the
   * cluster specification, including substituing max allowed values
   * if the specification asked for it.
   * @param role role
   * @param capability capability to set up
   */
  public void buildResourceRequirements(RoleStatus role, Resource capability) {
    // Set up resource requirements from role values
    String name = role.getName();
    ConfTreeOperations resources = getResourcesSnapshot();
    int cores = getResourceRequirement(resources,
                                       name,
                                       YARN_CORES,
                                       DEF_YARN_CORES,
                                       containerMaxCores);
    capability.setVirtualCores(cores);
    int ram = getResourceRequirement(resources, name,
                                     YARN_MEMORY,
                                     DEF_YARN_MEMORY,
                                     containerMaxMemory);
    capability.setMemory(ram);
  }

  /**
   * add a launched container to the node map for status responses
   * @param container id
   * @param node node details
   */
  private void addLaunchedContainer(Container container, RoleInstance node) {
    node.container = container;
    if (node.role == null) {
      throw new RuntimeException(
        "Unknown role for node " + node);
    }
    getLiveNodes().put(node.getContainerId(), node);
    //tell role history
    roleHistory.onContainerStarted(container);
  }

  /**
   * container start event
   * @param containerId container that is to be started
   * @return the role instance, or null if there was a problem
   */
  public synchronized RoleInstance onNodeManagerContainerStarted(ContainerId containerId) {
    try {
      return innerOnNodeManagerContainerStarted(containerId);
    } catch (YarnRuntimeException e) {
      log.error("NodeManager callback on started container {} failed",
                containerId,
                e);
      return null;
    }
  }

   /**
   * container start event handler -throwing an exception on problems
   * @param containerId container that is to be started
   * @return the role instance
   * @throws RuntimeException on problems
   */
  @VisibleForTesting
  public RoleInstance innerOnNodeManagerContainerStarted(ContainerId containerId) {
    incStartedCountainerCount();
    RoleInstance instance = activeContainers.get(containerId);
    if (instance == null) {
      //serious problem
      throw new YarnRuntimeException("Container not in active containers start "+
                containerId);
    }
    if (instance.role == null) {
      throw new YarnRuntimeException("Component instance has no instance name " +
                                     instance);
    }
    instance.startTime = now();
    RoleInstance starting = getStartingNodes().remove(containerId);
    if (null == starting) {
      throw new YarnRuntimeException(
        "Container "+ containerId +"%s is already started");
    }
    instance.state = ClusterDescription.STATE_LIVE;
    RoleStatus roleStatus = lookupRoleStatus(instance.roleId);
    roleStatus.incStarted();
    Container container = instance.container;
    addLaunchedContainer(container, instance);
    return instance;
  }

  /**
   * update the application state after a failure to start a container.
   * This is perhaps where blacklisting could be most useful: failure
   * to start a container is a sign of a more serious problem
   * than a later exit.
   *
   * -relayed from NMClientAsync.CallbackHandler 
   * @param containerId failing container
   * @param thrown what was thrown
   */
  public synchronized void onNodeManagerContainerStartFailed(ContainerId containerId,
                                                             Throwable thrown) {
    activeContainers.remove(containerId);
    incFailedCountainerCount();
    incStartFailedCountainerCount();
    RoleInstance instance = getStartingNodes().remove(containerId);
    if (null != instance) {
      RoleStatus roleStatus = lookupRoleStatus(instance.roleId);
      if (null != thrown) {
        instance.diagnostics = SliderUtils.stringify(thrown);
      }
      roleStatus.noteFailed(null);
      roleStatus.incStartFailed(); 
      getFailedNodes().put(containerId, instance);
      roleHistory.onNodeManagerContainerStartFailed(instance.container);
    }
  }

  /**
   * Is a role short lived by the threshold set for this application
   * @param instance instance
   * @return true if the instance is considered short live
   */
  @VisibleForTesting
  public boolean isShortLived(RoleInstance instance) {
    long time = now();
    long started = instance.startTime;
    boolean shortlived;
    if (started > 0) {
      long duration = time - started;
      shortlived = duration < startTimeThreshold;
    } else {
      // never even saw a start event
      shortlived = true;
    }
    return shortlived;
  }

  /**
   * Current time in milliseconds. Made protected for
   * the option to override it in tests.
   * @return the current time.
   */
  protected long now() {
    return System.currentTimeMillis();
  }

  /**
   * This is a very small class to send a triple result back from 
   * the completion operation
   */
  public static class NodeCompletionResult {
    public boolean surplusNode = false;
    public RoleInstance roleInstance;
    public boolean containerFailed;

    @Override
    public String toString() {
      final StringBuilder sb =
        new StringBuilder("NodeCompletionResult{");
      sb.append("surplusNode=").append(surplusNode);
      sb.append(", roleInstance=").append(roleInstance);
      sb.append(", containerFailed=").append(containerFailed);
      sb.append('}');
      return sb.toString();
    }
  }
  
  /**
   * handle completed node in the CD -move something from the live
   * server list to the completed server list
   * @param status the node that has just completed
   * @return NodeCompletionResult
   */
  public synchronized NodeCompletionResult onCompletedNode(ContainerStatus status) {
    return onCompletedNode(null, status);
  }
  
  /**
   * handle completed node in the CD -move something from the live
   * server list to the completed server list
   * @param amConf YarnConfiguration
   * @param status the node that has just completed
   * @return NodeCompletionResult
   */
  public synchronized NodeCompletionResult onCompletedNode(YarnConfiguration amConf,
      ContainerStatus status) {
    ContainerId containerId = status.getContainerId();
    NodeCompletionResult result = new NodeCompletionResult();
    RoleInstance roleInstance;

    if (containersBeingReleased.containsKey(containerId)) {
      log.info("Container was queued for release");
      Container container = containersBeingReleased.remove(containerId);
      RoleStatus roleStatus = lookupRoleStatus(container);
      log.info("decrementing role count for role {}", roleStatus.getName());
      roleStatus.decReleasing();
      roleStatus.decActual();
      roleStatus.incCompleted();
      roleHistory.onReleaseCompleted(container);

    } else if (surplusNodes.remove(containerId)) {
      //its a surplus one being purged
      result.surplusNode = true;
    } else {
      //a container has failed 
      result.containerFailed = true;
      roleInstance = activeContainers.remove(containerId);
      if (roleInstance != null) {
        //it was active, move it to failed 
        incFailedCountainerCount();
        failedNodes.put(containerId, roleInstance);
      } else {
        // the container may have been noted as failed already, so look
        // it up
        roleInstance = failedNodes.get(containerId);
      }
      if (roleInstance != null) {
        int roleId = roleInstance.roleId;
        log.info("Failed container in role {}", roleId);
        try {
          RoleStatus roleStatus = lookupRoleStatus(roleId);
          roleStatus.decActual();
          boolean shortLived = isShortLived(roleInstance);
          String message;
          if (roleInstance.container != null) {
            String user = null;
            try {
              user = SliderUtils.getCurrentUser().getShortUserName();
            } catch (IOException ioe) {
            }
            String completedLogsUrl = null;
            Container c = roleInstance.container;
            String url = null;
            if (amConf != null) {
              url = amConf.get(YarnConfiguration.YARN_LOG_SERVER_URL);
            }
            if (user != null && url != null) {
              completedLogsUrl = url
                  + "/" + c.getNodeId() + "/" + roleInstance.getContainerId() + "/ctx/" + user;
            }
            message = String.format("Failure %s on host %s" +
                (completedLogsUrl != null ? ", see %s" : ""), roleInstance.getContainerId(),
                c.getNodeId().getHost(), completedLogsUrl);
          } else {
            message = String.format("Failure %s",
                                    containerId.toString());
          }
          roleStatus.noteFailed(message);
          //have a look to see if it short lived
          if (shortLived) {
            roleStatus.incStartFailed();
          }
          
          if (roleInstance.container != null) {
            roleHistory.onFailedContainer(roleInstance.container, shortLived);
          }
          
        } catch (YarnRuntimeException e1) {
          log.error("Failed container of unknown role {}", roleId);
        }
      } else {
        //this isn't a known container.
        
        log.error("Notified of completed container {} that is not in the list" +
                  " of active or failed containers", containerId);
        completionOfUnknownContainerEvent.incrementAndGet();
      }
    }
    
    if (result.surplusNode) {
      //a surplus node
      return result;
    }
    
    //record the complete node's details; this pulls it from the livenode set 
    //remove the node
    ContainerId id = status.getContainerId();
    RoleInstance node = getLiveNodes().remove(id);
    if (node == null) {
      log.warn("Received notification of completion of unknown node {}", id);
      completionOfNodeNotInLiveListEvent.incrementAndGet();

    } else {
      node.state = ClusterDescription.STATE_DESTROYED;
      node.exitCode = status.getExitStatus();
      node.diagnostics = status.getDiagnostics();
      getCompletedNodes().put(id, node);
      result.roleInstance = node;
    }
    return result;
  }


  /**
   * Return the percentage done that Slider is to have YARN display in its
   * Web UI
   * @return an number from 0 to 100
   */
  public synchronized float getApplicationProgressPercentage() {
    float percentage;
    int desired = 0;
    float actual = 0;
    for (RoleStatus role : getRoleStatusMap().values()) {
      desired += role.getDesired();
      actual += role.getActual();
    }
    if (desired == 0) {
      percentage = 100;
    } else {
      percentage = actual / desired;
    }
    return percentage;
  }

  @Override
  public void refreshClusterStatus() {
    refreshClusterStatus(null);
  }
  
  /**
   * Update the cluster description with anything interesting
   * @param providerStatus status from the provider for the cluster info section
   */
  public void refreshClusterStatus(Map<String, String> providerStatus) {
    ClusterDescription cd = getClusterStatus();
    long now = now();
    cd.setInfoTime(StatusKeys.INFO_STATUS_TIME_HUMAN,
                   StatusKeys.INFO_STATUS_TIME_MILLIS,
                   now);
    if (providerStatus != null) {
      for (Map.Entry<String, String> entry : providerStatus.entrySet()) {
        cd.setInfo(entry.getKey(),entry.getValue());
      }
    }
    MapOperations infoOps = new MapOperations("info",cd.info);
    infoOps.mergeWithoutOverwrite(applicationInfo);
    SliderUtils.addBuildInfo(infoOps, "status");
    cd.statistics = new HashMap<String, Map<String, Integer>>();

    // build the map of node -> container IDs
    Map<String, List<String>> instanceMap = createRoleToInstanceMap();
    cd.instances = instanceMap;
    
    //build the map of node -> containers
    Map<String, Map<String, ClusterNode>> clusterNodes =
      createRoleToClusterNodeMap();
    cd.status = new HashMap<String, Object>();
    cd.status.put(ClusterDescriptionKeys.KEY_CLUSTER_LIVE, clusterNodes);


    for (RoleStatus role : getRoleStatusMap().values()) {
      String rolename = role.getName();
      List<String> instances = instanceMap.get(rolename);
      int nodeCount = instances != null ? instances.size(): 0;
      cd.setRoleOpt(rolename, ResourceKeys.COMPONENT_INSTANCES,
                    role.getDesired());
      cd.setRoleOpt(rolename, RoleKeys.ROLE_ACTUAL_INSTANCES, nodeCount);
      cd.setRoleOpt(rolename, ROLE_REQUESTED_INSTANCES, role.getRequested());
      cd.setRoleOpt(rolename, ROLE_RELEASING_INSTANCES, role.getReleasing());
      cd.setRoleOpt(rolename, ROLE_FAILED_INSTANCES, role.getFailed());
      cd.setRoleOpt(rolename, ROLE_FAILED_STARTING_INSTANCES, role.getStartFailed());
      Map<String, Integer> stats = role.buildStatistics();
      cd.statistics.put(rolename, stats);
    }

    Map<String, Integer> sliderstats = new HashMap<String, Integer>();
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_COMPLETED,
        completedContainerCount.get());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_FAILED,
        failedContainerCount.get());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_LIVE, liveNodes.size());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_STARTED,
        startedContainers.get());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_START_FAILED,
        startFailedContainers.get());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_SURPLUS,
        surplusContainers.get());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_UNKNOWN_COMPLETED,
        completionOfUnknownContainerEvent.get());
    cd.statistics.put(SliderKeys.COMPONENT_AM, sliderstats);
    
  }

  /**
   * Look at where the current node state is -and whether it should be changed
   */
  public synchronized List<AbstractRMOperation> reviewRequestAndReleaseNodes()
      throws SliderInternalStateException, TriggerClusterTeardownException {
    log.debug("in reviewRequestAndReleaseNodes()");
    List<AbstractRMOperation> allOperations =
      new ArrayList<AbstractRMOperation>();
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      if (!roleStatus.getExcludeFromFlexing()) {
        List<AbstractRMOperation> operations = reviewOneRole(roleStatus);
        allOperations.addAll(operations);
      }
    }
    return allOperations;
  }
  
  public void checkFailureThreshold(RoleStatus role) throws
                                                        TriggerClusterTeardownException {
    int failures = role.getFailed();

    if (failures > failureThreshold) {
      throw new TriggerClusterTeardownException(
        SliderExitCodes.EXIT_DEPLOYMENT_FAILED,
        ErrorStrings.E_UNSTABLE_CLUSTER +
        " - failed with role %s failing %d times (%d in startup); threshold is %d - last failure: %s",
        role.getName(),
        role.getFailed(),
        role.getStartFailed(),
        failureThreshold,
        role.getFailureMessage());
    }
  }
  
  /**
   * Look at the allocation status of one role, and trigger add/release
   * actions if the number of desired role instances doesnt equal 
   * (actual+pending)
   * @param role role
   * @return a list of operations
   * @throws SliderInternalStateException if the operation reveals that
   * the internal state of the application is inconsistent.
   */
  public List<AbstractRMOperation> reviewOneRole(RoleStatus role)
      throws SliderInternalStateException, TriggerClusterTeardownException {
    List<AbstractRMOperation> operations = new ArrayList<AbstractRMOperation>();
    int delta;
    String details;
    int expected;
    String name = role.getName();
    synchronized (role) {
      delta = role.getDelta();
      details = role.toString();
      expected = role.getDesired();
    }

    log.info(details);
    checkFailureThreshold(role);
    
    if (delta > 0) {
      log.info("{}: Asking for {} more nodes(s) for a total of {} ", name,
               delta, expected);
      //more workers needed than we have -ask for more
      for (int i = 0; i < delta; i++) {
        Resource capability = recordFactory.newResource();
        AMRMClient.ContainerRequest containerAsk =
          buildContainerResourceAndRequest(role, capability);
        log.info("Container ask is {}", containerAsk);
        if (containerAsk.getCapability().getMemory() >
            this.containerMaxMemory) {
          log.warn(
            "Memory requested: " + containerAsk.getCapability().getMemory() +
            " > " +
            this.containerMaxMemory);
        }
        operations.add(new ContainerRequestOperation(containerAsk));
      }
    } else if (delta < 0) {
      log.info("{}: Asking for {} fewer node(s) for a total of {}", name,
               -delta,
               expected);
      //reduce the number expected (i.e. subtract the delta)

      //then pick some containers to kill
      int excess = -delta;

      // get the nodes to release
      int roleId = role.getKey();
      List<NodeInstance> nodesForRelease =
        roleHistory.findNodesForRelease(roleId, excess);
      
      for (NodeInstance node : nodesForRelease) {
        RoleInstance possible = findRoleInstanceOnHost(node, roleId);
        if (possible == null) {
          throw new SliderInternalStateException(
            "Failed to find a container to release on node %s", node.hostname);
        }
        containerReleaseSubmitted(possible.container);
        operations.add(new ContainerReleaseOperation(possible.getId()));

      }
   
    }

    return operations;
  }


  /**
   * Find a container running on a specific host -looking
   * into the node ID to determine this.
   *
   * @param node node
   * @param roleId role the container must be in
   * @return a container or null if there are no containers on this host
   * that can be released.
   */
  private RoleInstance findRoleInstanceOnHost(NodeInstance node, int roleId) {
    Collection<RoleInstance> targets = cloneActiveContainerList();
    String hostname = node.hostname;
    for (RoleInstance ri : targets) {
      if (hostname.equals(RoleHistoryUtils.hostnameOf(ri.container))
                         && ri.roleId == roleId
        && containersBeingReleased.get(ri.getContainerId()) == null) {
        return ri;
      }
    }
    return null;
  }
  
  /**
   * Release all containers.
   * @return a list of operations to execute
   */
  public synchronized List<AbstractRMOperation> releaseAllContainers() {

    Collection<RoleInstance> targets = cloneActiveContainerList();
    log.info("Releasing {} containers", targets.size());
    List<AbstractRMOperation> operations =
      new ArrayList<AbstractRMOperation>(targets.size());
    for (RoleInstance instance : targets) {
      Container possible = instance.container;
      ContainerId id = possible.getId();
      if (!instance.released) {
        try {
          containerReleaseSubmitted(possible);
        } catch (SliderInternalStateException e) {
          log.warn("when releasing container {} :", possible, e);
        }
        operations.add(new ContainerReleaseOperation(id));
      }
    }
    return operations;
  }

  /**
   * Event handler for allocated containers: builds up the lists
   * of assignment actions (what to run where), and possibly
   * a list of release operations
   * @param allocatedContainers the containers allocated
   * @param assignments the assignments of roles to containers
   * @param releaseOperations any release operations
   */
  public synchronized void onContainersAllocated(List<Container> allocatedContainers,
                                    List<ContainerAssignment> assignments,
                                    List<AbstractRMOperation> releaseOperations) {
    assignments.clear();
    releaseOperations.clear();
    List<Container> ordered = roleHistory.prepareAllocationList(allocatedContainers);
    for (Container container : ordered) {
      String containerHostInfo = container.getNodeId().getHost()
                                 + ":" +
                                 container.getNodeId().getPort();
      int allocated;
      int desired;
      //get the role
      ContainerId cid = container.getId();
      RoleStatus role = lookupRoleStatus(container);
      

      //dec requested count
      role.decRequested();
      //inc allocated count -this may need to be dropped in a moment,
      // but us needed to update the logic below
      allocated = role.incActual();

      //look for (race condition) where we get more back than we asked
      desired = role.getDesired();

      roleHistory.onContainerAllocated( container, desired, allocated );

      if (allocated > desired) {
        log.info("Discarding surplus container {} on {}", cid,
                 containerHostInfo);
        releaseOperations.add(new ContainerReleaseOperation(cid));
        //register as a surplus node
        surplusNodes.add(cid);
        surplusContainers.incrementAndGet();
        //and, as we aren't binding it to role, dec that role's actual count
        role.decActual();
      } else {

        String roleName = role.getName();
        log.info("Assigning role {} to container" +
                 " {}," +
                 " on {}:{},",
                 roleName,
                 cid,
                 container.getNodeId().getHost(),
                 container.getNodeId().getPort()
                );

        assignments.add(new ContainerAssignment(container, role));
        //add to the history
        roleHistory.onContainerAssigned(container);
      }
    }
  }

  /**
   * Get diagnostics info about containers
   */
  public String getContainerDiagnosticInfo() {
    StringBuilder builder = new StringBuilder();
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      builder.append(roleStatus).append('\n');
    }
    return builder.toString();
  }

  /**
   * Event handler for the list of active containers on restart.
   * Sets the info key {@link StatusKeys#INFO_CONTAINERS_AM_RESTART}
   * to the size of the list passed down (and does not set it if none were)
   * @param liveContainers the containers allocated
   * @return true if a rebuild took place (even if size 0)
   * @throws RuntimeException on problems
   */
  private boolean rebuildModelFromRestart(List<Container> liveContainers) throws
                                                                          BadClusterStateException {
    if (liveContainers == null) {
      return false;
    }
    for (Container container : liveContainers) {
      addRestartedContainer(container);
    }
    clusterStatus.setInfo(StatusKeys.INFO_CONTAINERS_AM_RESTART,
                               Integer.toString(liveContainers.size()));
    return true;
  }

  /**
   * Add a restarted container by walking it through the create/submit/start
   * lifecycle, so building up the internal structures
   * @param container container that was running before the AM restarted
   * @throws RuntimeException on problems
   */
  private void addRestartedContainer(Container container) throws
                                                          BadClusterStateException {
    String containerHostInfo = container.getNodeId().getHost()
                               + ":" +
                               container.getNodeId().getPort();
    // get the container ID
    ContainerId cid = container.getId();
    
    // get the role
    int roleId = ContainerPriority.extractRole(container);
    RoleStatus role =
      lookupRoleStatus(roleId);
    // increment its count
    role.incActual();
    String roleName = role.getName();
    
    log.info("Rebuilding container {} in role {} on {},",
             cid,
             roleName,
             containerHostInfo);
    
    //update app state internal structures and maps

    RoleInstance instance = new RoleInstance(container);
    instance.command = roleName;
    instance.role = roleName;
    instance.roleId = roleId;
    instance.environment = new String[0];
    instance.container = container;
    instance.createTime = now();
    instance.state = ClusterDescription.STATE_LIVE;
    activeContainers.put(cid, instance);
    //role history gets told
    roleHistory.onContainerAssigned(container);
    // pretend the container has just had its start actions submitted
    containerStartSubmitted(container, instance);
    // now pretend it has just started
    innerOnNodeManagerContainerStarted(cid);
  }
}
