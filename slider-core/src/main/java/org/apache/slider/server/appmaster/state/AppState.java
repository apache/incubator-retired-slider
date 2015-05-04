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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ContainerPBImpl;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.api.ClusterDescriptionKeys;
import org.apache.slider.api.ClusterDescriptionOperations;
import org.apache.slider.api.ClusterNode;
import org.apache.slider.api.InternalKeys;
import org.apache.slider.api.ResourceKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.api.StatusKeys;
import org.apache.slider.api.types.ApplicationLivenessInformation;
import org.apache.slider.api.types.ComponentInformation;
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
import org.apache.slider.core.persist.AggregateConfSerDeser;
import org.apache.slider.core.persist.ConfTreeSerDeser;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.server.appmaster.management.MetricsAndMonitoring;
import org.apache.slider.server.appmaster.management.MetricsConstants;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.ContainerReleaseOperation;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.slider.api.ResourceKeys.DEF_YARN_CORES;
import static org.apache.slider.api.ResourceKeys.DEF_YARN_LABEL_EXPRESSION;
import static org.apache.slider.api.ResourceKeys.DEF_YARN_MEMORY;
import static org.apache.slider.api.ResourceKeys.YARN_CORES;
import static org.apache.slider.api.ResourceKeys.YARN_LABEL_EXPRESSION;
import static org.apache.slider.api.ResourceKeys.YARN_MEMORY;
import static org.apache.slider.api.RoleKeys.ROLE_FAILED_INSTANCES;
import static org.apache.slider.api.RoleKeys.ROLE_FAILED_STARTING_INSTANCES;
import static org.apache.slider.api.RoleKeys.ROLE_FAILED_RECENTLY_INSTANCES;
import static org.apache.slider.api.RoleKeys.ROLE_NODE_FAILED_INSTANCES;
import static org.apache.slider.api.RoleKeys.ROLE_PREEMPTED_INSTANCES;
import static org.apache.slider.api.RoleKeys.ROLE_RELEASING_INSTANCES;
import static org.apache.slider.api.RoleKeys.ROLE_REQUESTED_INSTANCES;
import static org.apache.slider.api.StateValues.STATE_CREATED;
import static org.apache.slider.api.StateValues.STATE_DESTROYED;
import static org.apache.slider.api.StateValues.STATE_LIVE;
import static org.apache.slider.api.StateValues.STATE_SUBMITTED;


/**
 * The model of all the ongoing state of a Slider AM.
 *
 * concurrency rules: any method which begins with <i>build</i>
 * is not synchronized and intended to be used during
 * initialization.
 */
public class AppState {
  protected static final Logger log =
    LoggerFactory.getLogger(AppState.class);
  
  private final AbstractRecordFactory recordFactory;

  private final MetricsAndMonitoring metricsAndMonitoring;

  /**
   * Flag set to indicate the application is live -this only happens
   * after the buildInstance operation
   */
  private boolean applicationLive = false;

  /**
   * The definition of the instance. Flexing updates the resources section
   * This is used as a synchronization point on activities that update
   * the CD, and also to update some of the structures that
   * feed in to the CD
   */
  private AggregateConf instanceDefinition;

  /**
   * Time the instance definition snapshots were created
   */
  private long snapshotTime;

  /**
   * Snapshot of the instance definition. This is fully
   * resolved.
   */
  private AggregateConf instanceDefinitionSnapshot;

  /**
   * Snapshot of the raw instance definition; unresolved and
   * without any patch of an AM into it.
   */
  private AggregateConf unresolvedInstanceDefinition;

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
  private Map<String, String> clientProperties = new HashMap<>();

  /**
   * This is a template of the cluster status
   */
  private ClusterDescription clusterStatusTemplate = new ClusterDescription();

  private final Map<Integer, RoleStatus> roleStatusMap =
    new ConcurrentHashMap<Integer, RoleStatus>();

  private final Map<String, ProviderRole> roles =
    new ConcurrentHashMap<String, ProviderRole>();

  private final Map<Integer, ProviderRole> rolePriorityMap = 
    new ConcurrentHashMap<Integer, ProviderRole>();

  /**
   * The master node.
   */
  private RoleInstance appMasterNode;

  /**
   * Hash map of the containers we have. This includes things that have
   * been allocated but are not live; it is a superset of the live list
   */
  private final ConcurrentMap<ContainerId, RoleInstance> ownedContainers =
    new ConcurrentHashMap<>();

  /**
   * Hash map of the containers we have released, but we
   * are still awaiting acknowledgements on. Any failure of these
   * containers is treated as a successful outcome
   */
  private final ConcurrentMap<ContainerId, Container> containersBeingReleased =
    new ConcurrentHashMap<>();
  
  /**
   * Counter for completed containers ( complete denotes successful or failed )
   */
  private final Counter completedContainerCount = new Counter();

  /**
   *   Count of failed containers

   */
  private final Counter failedContainerCount = new Counter();

  /**
   * # of started containers
   */
  private final Counter startedContainers = new Counter();

  /**
   * # of containers that failed to start 
   */
  private final Counter startFailedContainerCount = new Counter();

  /**
   * Track the number of surplus containers received and discarded
   */
  private final Counter surplusContainers = new Counter();


  /**
   * Track the number of requested Containers
   */
  private final Counter outstandingContainerRequests = new Counter();

  /**
   * Map of requested nodes. This records the command used to start it,
   * resources, etc. When container started callback is received,
   * the node is promoted from here to the containerMap
   */
  private final Map<ContainerId, RoleInstance> startingNodes =
    new ConcurrentHashMap<>();

  /**
   * List of completed nodes. This isn't kept in the CD as it gets too
   * big for the RPC responses. Indeed, we should think about how deep to get this
   */
  private final Map<ContainerId, RoleInstance> completedNodes
    = new ConcurrentHashMap<>();

  /**
   * Nodes that failed to start.
   * Again, kept out of the CD
   */
  private final Map<ContainerId, RoleInstance> failedNodes =
    new ConcurrentHashMap<>();

  /**
   * Nodes that came assigned to a role above that
   * which were asked for -this appears to happen
   */
  private final Set<ContainerId> surplusNodes = new HashSet<ContainerId>();

  /**
   * Map of containerID to cluster nodes, for status reports.
   * Access to this should be synchronized on the clusterDescription
   */
  private final Map<ContainerId, RoleInstance> liveNodes =
    new ConcurrentHashMap<>();
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
  private int nodeFailureThreshold = 3;
  
  private String logServerURL = "";

  /**
   * Selector of containers to release; application wide.
   */
  private ContainerReleaseSelector containerReleaseSelector;

  /**
   * Create an instance
   * @param recordFactory factory for YARN records
   * @param metricsAndMonitoring metrics and monitoring services
   */
  public AppState(AbstractRecordFactory recordFactory,
      MetricsAndMonitoring metricsAndMonitoring) {
    this.recordFactory = recordFactory;
    this.metricsAndMonitoring = metricsAndMonitoring;

    // register any metrics
    register(MetricsConstants.CONTAINERS_OUTSTANDING_REQUESTS, outstandingContainerRequests);
    register(MetricsConstants.CONTAINERS_SURPLUS, surplusContainers);
    register(MetricsConstants.CONTAINERS_STARTED, startedContainers);
    register(MetricsConstants.CONTAINERS_COMPLETED, completedContainerCount);
    register(MetricsConstants.CONTAINERS_FAILED, failedContainerCount);
    register(MetricsConstants.CONTAINERS_START_FAILED, startFailedContainerCount);
  }

  private void register(String name, Counter counter) {
    this.metricsAndMonitoring.getMetrics().register(
        MetricRegistry.name(AppState.class,
            name), counter);
  }

  public long getFailedCountainerCount() {
    return failedContainerCount.getCount();
  }

  /**
   * Increment the count
   */
  public void incFailedCountainerCount() {
    failedContainerCount.inc();
  }

  public long getStartFailedCountainerCount() {
    return startFailedContainerCount.getCount();
  }

  /**
   * Increment the count and return the new value
   */
  public void incStartedCountainerCount() {
    startedContainers.inc();
  }

  public long getStartedCountainerCount() {
    return startedContainers.getCount();
  }

  /**
   * Increment the count and return the new value
   */
  public void incStartFailedCountainerCount() {
    startFailedContainerCount.inc();
  }

  public AtomicInteger getCompletionOfNodeNotInLiveListEvent() {
    return completionOfNodeNotInLiveListEvent;
  }

  public AtomicInteger getCompletionOfUnknownContainerEvent() {
    return completionOfUnknownContainerEvent;
  }


  public Map<Integer, RoleStatus> getRoleStatusMap() {
    return roleStatusMap;
  }
  
  protected Map<String, ProviderRole> getRoleMap() {
    return roles;
  }

  public Map<Integer, ProviderRole> getRolePriorityMap() {
    return rolePriorityMap;
  }

  private Map<ContainerId, RoleInstance> getStartingNodes() {
    return startingNodes;
  }

  private Map<ContainerId, RoleInstance> getCompletedNodes() {
    return completedNodes;
  }


  public Map<ContainerId, RoleInstance> getFailedNodes() {
    return failedNodes;
  }


  public Map<ContainerId, RoleInstance> getLiveNodes() {
    return liveNodes;
  }

  /**
   * Get the current view of the cluster status.
   * <p>
   *   Calls to {@link #refreshClusterStatus()} trigger a
   *   refresh of this field.
   * <p>
   * This is read-only
   * to the extent that changes here do not trigger updates in the
   * application state. 
   * @return the cluster status
   */
  public synchronized ClusterDescription getClusterStatus() {
    return clusterStatus;
  }

  @VisibleForTesting
  protected synchronized void setClusterStatus(ClusterDescription clusterDesc) {
    this.clusterStatus = clusterDesc;
  }

  /**
   * Set the instance definition -this also builds the (now obsolete)
   * cluster specification from it.
   * 
   * Important: this is for early binding and must not be used after the build
   * operation is complete. 
   * @param definition initial definition
   * @throws BadConfigException
   */
  public synchronized void setInitialInstanceDefinition(AggregateConf definition)
      throws BadConfigException, IOException {
    log.debug("Setting initial instance definition");
    // snapshot the definition
    AggregateConfSerDeser serDeser = new AggregateConfSerDeser();

    unresolvedInstanceDefinition = serDeser.fromInstance(definition);
    
    this.instanceDefinition = serDeser.fromInstance(definition);
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


  public ConfTreeOperations getResourcesSnapshot() {
    return resourcesSnapshot;
  }


  public ConfTreeOperations getAppConfSnapshot() {
    return appConfSnapshot;
  }


  public ConfTreeOperations getInternalsSnapshot() {
    return internalsSnapshot;
  }

  public boolean isApplicationLive() {
    return applicationLive;
  }

  public long getSnapshotTime() {
    return snapshotTime;
  }

  public AggregateConf getInstanceDefinitionSnapshot() {
    return instanceDefinitionSnapshot;
  }

  public AggregateConf getUnresolvedInstanceDefinition() {
    return unresolvedInstanceDefinition;
  }

  /**
   * Build up the application state
   * @param instanceDefinition definition of the applicatin instance
   * @param appmasterConfig
   * @param publishedProviderConf any configuration info to be published by a provider
   * @param providerRoles roles offered by a provider
   * @param fs filesystem
   * @param historyDir directory containing history files
   * @param liveContainers list of live containers supplied on an AM restart
   * @param applicationInfo app info to retain for web views
   * @param releaseSelector selector of containers to release
   */
  public synchronized void buildInstance(AggregateConf instanceDefinition,
      Configuration appmasterConfig,
      Configuration publishedProviderConf,
      List<ProviderRole> providerRoles,
      FileSystem fs,
      Path historyDir,
      List<Container> liveContainers,
      Map<String, String> applicationInfo,
      ContainerReleaseSelector releaseSelector)
      throws  BadClusterStateException, BadConfigException, IOException {
    Preconditions.checkArgument(instanceDefinition != null);
    Preconditions.checkArgument(releaseSelector != null);

    log.debug("Building application state");
    this.publishedProviderConf = publishedProviderConf;
    this.applicationInfo = applicationInfo != null ? applicationInfo
                                                   : new HashMap<String, String>();

    clientProperties = new HashMap<>();
    containerReleaseSelector = releaseSelector;


    Set<String> confKeys = ConfigHelper.sortedConfigKeys(publishedProviderConf);

    //  Add the -site configuration properties
    for (String key : confKeys) {
      String val = publishedProviderConf.get(key);
      clientProperties.put(key, val);
    }

    // set the cluster specification (once its dependency the client properties
    // is out the way
    setInitialInstanceDefinition(instanceDefinition);

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
        log.info("Adding role {}", name);
        MapOperations resComponent = resources.getComponent(name);
        ProviderRole dynamicRole =
            createDynamicProviderRole(name, resComponent);
        buildRole(dynamicRole);
        providerRoles.add(dynamicRole);
      }
    }
    //then pick up the requirements
    buildRoleRequirementsFromResources();


    //set the livespan
    MapOperations globalResOpts =
        instanceDefinition.getResourceOperations().getGlobalOptions();
    
    startTimeThreshold = globalResOpts.getOptionInt(
        InternalKeys.INTERNAL_CONTAINER_FAILURE_SHORTLIFE,
        InternalKeys.DEFAULT_INTERNAL_CONTAINER_FAILURE_SHORTLIFE);

    failureThreshold = globalResOpts.getOptionInt(
        ResourceKeys.CONTAINER_FAILURE_THRESHOLD,
        ResourceKeys.DEFAULT_CONTAINER_FAILURE_THRESHOLD);
    nodeFailureThreshold = globalResOpts.getOptionInt(
        ResourceKeys.NODE_FAILURE_THRESHOLD,
        ResourceKeys.DEFAULT_NODE_FAILURE_THRESHOLD);
    initClusterStatus();


    // add the roles
    roleHistory = new RoleHistory(providerRoles);
    roleHistory.onStart(fs, historyDir);

    //rebuild any live containers
    rebuildModelFromRestart(liveContainers);

    // any am config options to pick up
    logServerURL = appmasterConfig.get(YarnConfiguration.YARN_LOG_SERVER_URL, "");
    
    //mark as live
    applicationLive = true;
  }

  public void initClusterStatus() {
    //copy into cluster status. 
    ClusterDescription status = ClusterDescription.copy(clusterStatusTemplate);
    status.state = STATE_CREATED;
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
    status.state = STATE_LIVE;

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
    int priority = SliderUtils.parseAndValidate("value of " + name + " " +
                                                ResourceKeys.COMPONENT_PRIORITY,
        priOpt, 0, 1, -1);
    String placementOpt = component.getOption(
        ResourceKeys.COMPONENT_PLACEMENT_POLICY,
        Integer.toString(PlacementPolicy.DEFAULT));
    int placement = SliderUtils.parseAndValidate("value of " + name + " " +
                                                 ResourceKeys.COMPONENT_PLACEMENT_POLICY,
        placementOpt, 0, 0, -1);
    int placementTimeout =
        component.getOptionInt(ResourceKeys.PLACEMENT_ESCALATE_DELAY,
            ResourceKeys.DEFAULT_PLACEMENT_ESCALATE_DELAY_SECONDS);
    ProviderRole newRole = new ProviderRole(name,
        priority,
        placement,
        getNodeFailureThresholdForRole(name),
        placementTimeout);
    log.info("New {} ", newRole);
    return newRole;
  }


  /**
   * Actions to perform when an instance definition is updated
   * Currently: 
   * <ol>
   *   <li>
   *     resolve the configuration
   *   </li>
   *   <li>
   *     update the cluster spec derivative
   *   </li>
   * </ol>
   *  
   * @throws BadConfigException
   */
  private synchronized void onInstanceDefinitionUpdated() throws
                                                          BadConfigException,
                                                          IOException {
    log.debug("Instance definition updated");
    //note the time 
    snapshotTime = now();
    
    // resolve references if not already done
    instanceDefinition.resolve();

    // force in the AM desired state values
    ConfTreeOperations resources =
        instanceDefinition.getResourceOperations();
    if (resources.getComponent(SliderKeys.COMPONENT_AM) != null) {
      resources.setComponentOpt(
          SliderKeys.COMPONENT_AM, ResourceKeys.COMPONENT_INSTANCES, "1");
    }


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
    instanceDefinitionSnapshot.setName(instanceDefinition.getName());

    clusterStatusTemplate =
      ClusterDescriptionOperations.buildFromInstanceDefinition(
          instanceDefinition);
    

//     Add the -site configuration properties
    for (Map.Entry<String, String> prop : clientProperties.entrySet()) {
      clusterStatusTemplate.clientProperties.put(prop.getKey(), prop.getValue());
    }
    
  }
  
  /**
   * The resource configuration is updated -review and update state.
   * @param resources updated resources specification
   * @return a list of any dynamically added provider roles
   * (purely for testing purposes)
   */
  public synchronized List<ProviderRole> updateResourceDefinitions(ConfTree resources)
      throws BadConfigException, IOException {
    log.debug("Updating resources to {}", resources);
    // snapshot the (possibly unresolved) values
    ConfTreeSerDeser serDeser = new ConfTreeSerDeser();
    unresolvedInstanceDefinition.setResources(
        serDeser.fromInstance(resources));
    // assign another copy under the instance definition for resolving
    // and then driving application size
    instanceDefinition.setResources(serDeser.fromInstance(resources));
    onInstanceDefinitionUpdated();
 
    // propagate the role table
    Map<String, Map<String, String>> updated = resources.components;
    getClusterStatus().roles = SliderUtils.deepClone(updated);
    getClusterStatus().updateTime = now();
    return buildRoleRequirementsFromResources();
  }

  /**
   * build the role requirements from the cluster specification
   * @return a list of any dynamically added provider roles
   */
  private List<ProviderRole> buildRoleRequirementsFromResources() throws BadConfigException {

    List<ProviderRole> newRoles = new ArrayList<>(0);
    
    //now update every role's desired count.
    //if there are no instance values, that role count goes to zero

    ConfTreeOperations resources =
        instanceDefinition.getResourceOperations();

    // Add all the existing roles
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      if (roleStatus.isExcludeFromFlexing()) {
        // skip inflexible roles, e.g AM itself
        continue;
      }
      int currentDesired = roleStatus.getDesired();
      String role = roleStatus.getName();
      MapOperations comp =
          resources.getComponent(role);
      int desiredInstanceCount = getDesiredInstanceCount(resources, role);
      if (desiredInstanceCount == 0) {
        log.info("Role {} has 0 instances specified", role);
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
        MapOperations component = resources.getComponent(name);
        ProviderRole dynamicRole = createDynamicProviderRole(name,
            component);
        RoleStatus roleStatus = buildRole(dynamicRole);
        roleStatus.setDesired(getDesiredInstanceCount(resources, name));
        log.info("New role {}", roleStatus);
        roleHistory.addNewProviderRole(dynamicRole);
        newRoles.add(dynamicRole);
      }
    }
    return newRoles;
  }

  /**
   * Get the desired instance count of a role, rejecting negative values
   * @param resources resource map
   * @param role role name
   * @return the instance count
   * @throws BadConfigException if the count is negative
   */
  private int getDesiredInstanceCount(ConfTreeOperations resources,
      String role) throws BadConfigException {
    int desiredInstanceCount =
      resources.getComponentOptInt(role, ResourceKeys.COMPONENT_INSTANCES, 0);

    if (desiredInstanceCount < 0) {
      log.error("Role {} has negative desired instances : {}", role,
          desiredInstanceCount);
      throw new BadConfigException(
          "Negative instance count (%) requested for component %s",
          desiredInstanceCount, role);
    }
    return desiredInstanceCount;
  }

  /**
   * Add knowledge of a role.
   * This is a build-time operation that is not synchronized, and
   * should be used while setting up the system state -before servicing
   * requests.
   * @param providerRole role to add
   * @return the role status built up
   * @throws BadConfigException if a role of that priority already exists
   */
  public RoleStatus buildRole(ProviderRole providerRole) throws BadConfigException {
    //build role status map
    int priority = providerRole.id;
    if (roleStatusMap.containsKey(priority)) {
      throw new BadConfigException("Duplicate Provider Key: %s and %s",
                                   providerRole,
                                   roleStatusMap.get(priority));
    }
    RoleStatus roleStatus = new RoleStatus(providerRole);
    roleStatusMap.put(priority, roleStatus);
    roles.put(providerRole.name, providerRole);
    rolePriorityMap.put(priority, providerRole);
    return roleStatus;
  }

  /**
   * build up the special master node, which lives
   * in the live node set but has a lifecycle bonded to the AM
   * @param containerId the AM master
   * @param host hostname
   * @param amPort port
   * @param nodeHttpAddress http address: may be null
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
    am.roleId = SliderKeys.ROLE_AM_PRIORITY_INDEX;
    am.createTime = System.currentTimeMillis();
    am.startTime = System.currentTimeMillis();
    appMasterNode = am;
    //it is also added to the set of live nodes
    getLiveNodes().put(containerId, am);
    putOwnedContainer(containerId, am);
    
    // patch up the role status
    RoleStatus roleStatus = roleStatusMap.get(
        (SliderKeys.ROLE_AM_PRIORITY_INDEX));
    roleStatus.setDesired(1);
    roleStatus.incActual();
    roleStatus.incStarted();
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
    appMasterNode.state = STATE_LIVE;
  }

  public RoleInstance getAppMasterNode() {
    return appMasterNode;
  }


  public RoleStatus lookupRoleStatus(int key) {
    RoleStatus rs = getRoleStatusMap().get(key);
    if (rs == null) {
      throw new RuntimeException("Cannot find role for role ID " + key);
    }
    return rs;
  }
  
  public RoleStatus lookupRoleStatus(Container c) throws YarnRuntimeException {
    return lookupRoleStatus(ContainerPriority.extractRole(c));
  }

  /**
   * Get a deep clone of the role status list. Concurrent events may mean this
   * list (or indeed, some of the role status entries) may be inconsistent
   * @return a snapshot of the role status entries
   */
  public List<RoleStatus> cloneRoleStatusList() {
    Collection<RoleStatus> statuses = roleStatusMap.values();
    List<RoleStatus> statusList = new ArrayList<RoleStatus>(statuses.size());
    try {
      for (RoleStatus status : statuses) {
        statusList.add((RoleStatus)(status.clone()));
      }
    } catch (CloneNotSupportedException e) {
      log.warn("Unexpected cloning failure: {}", e, e);
    }
    return statusList;
  }


  public RoleStatus lookupRoleStatus(String name) throws YarnRuntimeException {
    ProviderRole providerRole = roles.get(name);
    if (providerRole == null) {
      throw new YarnRuntimeException("Unknown role " + name);
    }
    return lookupRoleStatus(providerRole.id);
  }


  /**
   * Clone the list of active (==owned) containers
   * @return the list of role instances representing all owned containers
   */
  public synchronized List<RoleInstance> cloneOwnedContainerList() {
    Collection<RoleInstance> values = ownedContainers.values();
    return new ArrayList<RoleInstance>(values);
  }

  /**
   * Get the number of active (==owned) containers
   * @return
   */
  public int getNumOwnedContainers() {
    return ownedContainers.size();
  }
  
  /**
   * Look up an active container: any container that the AM has, even
   * if it is not currently running/live
   */
  public RoleInstance getOwnedContainer(ContainerId id) {
    return ownedContainers.get(id);
  }

  /**
   * Remove an owned container
   * @param id container ID
   * @return the instance removed
   */
  private RoleInstance removeOwnedContainer(ContainerId id) {
    return ownedContainers.remove(id);
  }

  /**
   * set/update an owned container
   * @param id container ID
   * @param instance
   * @return
   */
  private RoleInstance putOwnedContainer(ContainerId id,
      RoleInstance instance) {
    return ownedContainers.put(id, instance);
  }

  /**
   * Clone the live container list. This is synchronized.
   * @return a snapshot of the live node list
   */
  public synchronized List<RoleInstance> cloneLiveContainerInfoList() {
    List<RoleInstance> allRoleInstances;
    Collection<RoleInstance> values = getLiveNodes().values();
    allRoleInstances = new ArrayList<>(values);
    return allRoleInstances;
  }

  /**
   * Lookup live instance by string value of container ID
   * @param containerId container ID as a string
   * @return the role instance for that container
   * @throws NoSuchNodeException if it does not exist
   */
  public synchronized RoleInstance getLiveInstanceByContainerID(String containerId)
      throws NoSuchNodeException {
    Collection<RoleInstance> nodes = getLiveNodes().values();
    return findNodeInCollection(containerId, nodes);
  }

  /**
   * Lookup owned instance by string value of container ID
   * @param containerId container ID as a string
   * @return the role instance for that container
   * @throws NoSuchNodeException if it does not exist
   */
  public synchronized RoleInstance getOwnedInstanceByContainerID(String containerId)
      throws NoSuchNodeException {
    Collection<RoleInstance> nodes = ownedContainers.values();
    return findNodeInCollection(containerId, nodes);
  }

  /**
   * Iterate through a collection of role instances to find one with a
   * specific (string) container ID
   * @param containerId container ID as a string
   * @param nodes collection
   * @return the found node 
   * @throws NoSuchNodeException if there was no match
   */
  private RoleInstance findNodeInCollection(String containerId,
      Collection<RoleInstance> nodes) throws NoSuchNodeException {
    RoleInstance found = null;
    for (RoleInstance node : nodes) {
      if (containerId.equals(node.id)) {
        found = node;
        break;
      }
    }
    if (found != null) {
      return found;
    } else {
      //at this point: no node
      throw new NoSuchNodeException("Unknown node: " + containerId);
    }
  }


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
   * enum nodes by role ID, from either the owned or live node list
   * @param roleId role the container must be in
   * @param owned flag to indicate "use owned list" rather than the smaller
   * "live" list
   * @return a list of nodes, may be empty
   */
  public synchronized List<RoleInstance> enumNodesWithRoleId(int roleId,
      boolean owned) {
    List<RoleInstance> nodes = new ArrayList<RoleInstance>();
    Collection<RoleInstance> allRoleInstances;
    allRoleInstances = owned ? ownedContainers.values() : liveNodes.values();
    for (RoleInstance node : allRoleInstances) {
      if (node.roleId == roleId) {
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
   * Build a map of role->nodename->node-info
   * 
   * @return the map of Role name to list of Cluster Nodes
   */
  public synchronized Map<String, Map<String, ClusterNode>> createRoleToClusterNodeMap() {
    Map<String, Map<String, ClusterNode>> map = new HashMap<>();
    for (RoleInstance node : getLiveNodes().values()) {
      
      Map<String, ClusterNode> containers = map.get(node.role);
      if (containers == null) {
        containers = new HashMap<String, ClusterNode>();
        map.put(node.role, containers);
      }
      ClusterNode clusterNode = node.toClusterNode();
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
    instance.state = STATE_SUBMITTED;
    instance.container = container;
    instance.createTime = now();
    getStartingNodes().put(container.getId(), instance);
    putOwnedContainer(container.getId(), instance);
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
    RoleInstance instance = getOwnedContainer(id);
    if (instance == null) {
      throw new SliderInternalStateException(
        "No active container with ID " + id);
    }
    //verify that it isn't already released
    if (containersBeingReleased.containsKey(id)) {
      throw new SliderInternalStateException(
        "Container %s already queued for release", id);
    }
    instance.released = true;
    containersBeingReleased.put(id, instance.container);
    RoleStatus role = lookupRoleStatus(instance.roleId);
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
    String labelExpression = getLabelExpression(role);
    //get the role history to select a suitable node, if available
    AMRMClient.ContainerRequest containerRequest =
      createContainerRequest(role, capability, labelExpression);
    return  containerRequest;
  }

  /**
   * Create a container request.
   * Update internal state, such as the role request count
   * This is where role history information will be used for placement decisions -
   * @param role role
   * @param resource requirements
   * @param labelExpression label expression to satisfy
   * @return the container request to submit
   */
  private AMRMClient.ContainerRequest createContainerRequest(RoleStatus role,
                                                            Resource resource,
                                                            String labelExpression) {
    
    
    AMRMClient.ContainerRequest request;
    request = roleHistory.requestNode(role, resource, labelExpression);
    incrementRequestCount(role);
    return request;
  }

  /**
   * Increment the request count of a role.
   * <p>
   *   Also updates application state counters
   * @param role role being requested.
   */
  protected void incrementRequestCount(RoleStatus role) {
    role.incRequested();
    incOutstandingContainerRequests();
  }


  /**
   * dec requested count of a role
   * <p>
   *   Also updates application state counters.
   * @param role role to decrement
   */
  protected synchronized void decrementRequestCount(RoleStatus role) {
    role.decRequested();
  }

  /**
   * Inc #of outstanding requests.
   */
  private void incOutstandingContainerRequests() {
    synchronized (outstandingContainerRequests) {
      outstandingContainerRequests.inc();
    }
  }

  /**
   * Decrement the number of outstanding requests. This never goes below zero.
   */
  private void decOutstandingContainerRequests() {
    synchronized (outstandingContainerRequests) {
      if (outstandingContainerRequests.getCount() > 0) {
        // decrement but never go below zero
        outstandingContainerRequests.dec();
      }
    }
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
   * Extract the label expression for this role.
   * @param role role
   */
  public String getLabelExpression(RoleStatus role) {
    // Set up resource requirements from role values
    String name = role.getName();
    ConfTreeOperations resources = getResourcesSnapshot();
    return resources.getComponentOpt(name, YARN_LABEL_EXPRESSION, DEF_YARN_LABEL_EXPRESSION);
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
    RoleInstance instance = getOwnedContainer(containerId);
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
    instance.state = STATE_LIVE;
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
    removeOwnedContainer(containerId);
    incFailedCountainerCount();
    incStartFailedCountainerCount();
    RoleInstance instance = getStartingNodes().remove(containerId);
    if (null != instance) {
      RoleStatus roleStatus = lookupRoleStatus(instance.roleId);
      String text;
      if (null != thrown) {
        text = SliderUtils.stringify(thrown);
      } else {
        text = "container start failure";
      }
      instance.diagnostics = text;
      roleStatus.noteFailed(true, text, ContainerOutcome.Failed);
      getFailedNodes().put(containerId, instance);
      roleHistory.onNodeManagerContainerStartFailed(instance.container);
    }
  }

  public synchronized void onNodesUpdated(List<NodeReport> updatedNodes) {
    roleHistory.onNodesUpdated(updatedNodes);
  }

  /**
   * Is a role short lived by the threshold set for this application
   * @param instance instance
   * @return true if the instance is considered short lived
   */
  @VisibleForTesting
  public boolean isShortLived(RoleInstance instance) {
    long time = now();
    long started = instance.startTime;
    boolean shortlived;
    if (started > 0) {
      long duration = time - started;
      shortlived = duration < (startTimeThreshold * 1000);
      log.info("Duration {} and startTimeThreshold {}", duration, startTimeThreshold);
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
   * This is a very small class to send a multiple result back from 
   * the completion operation
   */
  public static class NodeCompletionResult {
    public boolean surplusNode = false;
    public RoleInstance roleInstance;
    // did the container fail for *any* reason?
    public boolean containerFailed = false;
    // detailed outcome on the container failure
    public ContainerOutcome outcome = ContainerOutcome.Completed;
    public int exitStatus = 0;
    public boolean unknownNode = false;

  
    public String toString() {
      final StringBuilder sb =
        new StringBuilder("NodeCompletionResult{");
      sb.append("surplusNode=").append(surplusNode);
      sb.append(", roleInstance=").append(roleInstance);
      sb.append(", exitStatus=").append(exitStatus);
      sb.append(", containerFailed=").append(containerFailed);
      sb.append(", outcome=").append(outcome);
      sb.append(", unknownNode=").append(unknownNode);
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
    ContainerId containerId = status.getContainerId();
    NodeCompletionResult result = new NodeCompletionResult();
    RoleInstance roleInstance;

    int exitStatus = status.getExitStatus();
    result.exitStatus = exitStatus;
    if (containersBeingReleased.containsKey(containerId)) {
      log.info("Container was queued for release : {}", containerId);
      Container container = containersBeingReleased.remove(containerId);
      RoleStatus roleStatus = lookupRoleStatus(container);
      int releasing = roleStatus.decReleasing();
      int actual = roleStatus.decActual();
      int completedCount = roleStatus.incCompleted();
      log.info("decrementing role count for role {} to {}; releasing={}, completed={}",
          roleStatus.getName(),
          actual,
          releasing,
          completedCount);
      result.outcome = ContainerOutcome.Completed;
      roleHistory.onReleaseCompleted(container);

    } else if (surplusNodes.remove(containerId)) {
      //its a surplus one being purged
      result.surplusNode = true;
    } else {
      // a container has failed or been killed
      // use the exit code to determine the outcome
      result.containerFailed = true;
      result.outcome = ContainerOutcome.fromExitStatus(exitStatus);

      roleInstance = removeOwnedContainer(containerId);
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
        String rolename = roleInstance.role;
        log.info("Failed container in role[{}] : {}", roleId, rolename);
        try {
          RoleStatus roleStatus = lookupRoleStatus(roleId);
          roleStatus.decActual();
          boolean shortLived = isShortLived(roleInstance);
          String message;
          Container failedContainer = roleInstance.container;

          //build the failure message
          if (failedContainer != null) {
            String completedLogsUrl = getLogsURLForContainer(failedContainer);
            message = String.format("Failure %s on host %s (%d): %s",
                roleInstance.getContainerId().toString(),
                failedContainer.getNodeId().getHost(),
                exitStatus,
                completedLogsUrl);
          } else {
            message = String.format("Failure %s (%d)", containerId, exitStatus);
          }
          roleStatus.noteFailed(shortLived, message, result.outcome);
          long failed = roleStatus.getFailed();
          log.info("Current count of failed role[{}] {} =  {}",
              roleId, rolename, failed);
          if (failedContainer != null) {
            roleHistory.onFailedContainer(failedContainer, shortLived, result.outcome);
          }

        } catch (YarnRuntimeException e1) {
          log.error("Failed container of unknown role {}", roleId);
        }
      } else {
        //this isn't a known container.

        log.error("Notified of completed container {} that is not in the list" +
            " of active or failed containers", containerId);
        completionOfUnknownContainerEvent.incrementAndGet();
        result.unknownNode = true;
      }
    }

    if (result.surplusNode) {
      //a surplus node
      return result;
    }

    //record the complete node's details; this pulls it from the livenode set 
    //remove the node
    ContainerId id = status.getContainerId();
    log.info("Removing node ID {}", id);
    RoleInstance node = getLiveNodes().remove(id);
    if (node != null) {
      node.state = STATE_DESTROYED;
      node.exitCode = exitStatus;
      node.diagnostics = status.getDiagnostics();
      getCompletedNodes().put(id, node);
      result.roleInstance = node;
    } else {
      // not in the list
      log.warn("Received notification of completion of unknown node {}", id);
      completionOfNodeNotInLiveListEvent.incrementAndGet();
    }
    
    // and the active node list if present
    removeOwnedContainer(containerId);
    
    // finally, verify the node doesn't exist any more
    assert !containersBeingReleased.containsKey(
        containerId) : "container still in release queue";
    assert !getLiveNodes().containsKey(
        containerId) : " container still in live nodes";
    assert getOwnedContainer(containerId) ==
           null : "Container still in active container list";

    return result;
  }

  /**
   * Get the URL log for a container
   * @param c container
   * @return the URL or "" if it cannot be determined
   */
  protected String getLogsURLForContainer(Container c) {
    if (c==null) {
      return null;
    }
    String user = null;
    try {
      user = SliderUtils.getCurrentUser().getShortUserName();
    } catch (IOException ignored) {
    }
    String completedLogsUrl = "";
    String url = logServerURL;
    if (user != null && SliderUtils.isSet(url)) {
      completedLogsUrl = url
          + "/" + c.getNodeId() + "/" + c.getId() + "/ctx/" + user;
    }
    return completedLogsUrl;
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

  /**
   * Update the cluster description with the current application state
   */

  public ClusterDescription refreshClusterStatus() {
    return refreshClusterStatus(null);
  }
  
  /**
   * Update the cluster description with the current application state
   * @param providerStatus status from the provider for the cluster info section
   */
  public synchronized ClusterDescription refreshClusterStatus(Map<String, String> providerStatus) {
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
    MapOperations infoOps = new MapOperations("info", cd.info);
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
      cd.setRoleOpt(rolename, ROLE_FAILED_RECENTLY_INSTANCES, role.getFailedRecently());
      cd.setRoleOpt(rolename, ROLE_NODE_FAILED_INSTANCES, role.getNodeFailed());
      cd.setRoleOpt(rolename, ROLE_PREEMPTED_INSTANCES, role.getPreempted());
      Map<String, Integer> stats = role.buildStatistics();
      cd.statistics.put(rolename, stats);
    }

    
    Map<String, Integer> sliderstats = getLiveStatistics();
    cd.statistics.put(SliderKeys.COMPONENT_AM, sliderstats);
    
    // liveness
    cd.liveness = getApplicationLivenessInformation();
    
    return cd;
  }

  /**
   * get application liveness information
   * @return a snapshot of the current liveness information
   */  
  public ApplicationLivenessInformation getApplicationLivenessInformation() {
    ApplicationLivenessInformation li = new ApplicationLivenessInformation();
    int outstanding = (int) outstandingContainerRequests.getCount();
    li.requestsOutstanding = outstanding;
    li.allRequestsSatisfied = outstanding <= 0;
    return li;
  }
  
  /**
   * Get the live statistics map
   * @return a map of statistics values, defined in the {@link StatusKeys}
   * keylist.
   */
  protected Map<String, Integer> getLiveStatistics() {
    Map<String, Integer> sliderstats = new HashMap<>();
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_LIVE,
        liveNodes.size());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_COMPLETED,
        (int)completedContainerCount.getCount());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_FAILED,
        (int)failedContainerCount.getCount());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_STARTED,
        (int)startedContainers.getCount());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_START_FAILED,
        (int) startFailedContainerCount.getCount());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_SURPLUS,
        (int)surplusContainers.getCount());
    sliderstats.put(StatusKeys.STATISTICS_CONTAINERS_UNKNOWN_COMPLETED,
        completionOfUnknownContainerEvent.get());
    return sliderstats;
  }

  /**
   * Get a snapshot of component information.
   * <p>
   *   This does <i>not</i> include any container list, which 
   *   is more expensive to create.
   * @return a map of current role status values.
   */
  public Map<String, ComponentInformation> getComponentInfoSnapshot() {

    Map<Integer, RoleStatus> statusMap = getRoleStatusMap();
    Map<String, ComponentInformation> results =
        new HashMap<String, ComponentInformation>(
            statusMap.size());

    for (RoleStatus status : statusMap.values()) {
      String name = status.getName();
      ComponentInformation info = status.serialize();
      results.put(name, info);
    }
    return results;
  }

  /**
   * Look at where the current node state is -and whether it should be changed
   */
  public synchronized List<AbstractRMOperation> reviewRequestAndReleaseNodes()
      throws SliderInternalStateException, TriggerClusterTeardownException {
    log.debug("in reviewRequestAndReleaseNodes()");
    List<AbstractRMOperation> allOperations = new ArrayList<AbstractRMOperation>();
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      if (!roleStatus.isExcludeFromFlexing()) {
        List<AbstractRMOperation> operations = reviewOneRole(roleStatus);
        allOperations.addAll(operations);
      }
    }
    return allOperations;
  }

  /**
   * Check the "recent" failure threshold for a role
   * @param role role to examine
   * @throws TriggerClusterTeardownException if the role
   * has failed too many times
   */
  private void checkFailureThreshold(RoleStatus role)
      throws TriggerClusterTeardownException {
    long failures = role.getFailedRecently();
    int threshold = getFailureThresholdForRole(role);
    log.debug("Failure count of component: {}: {}, threshold={}",
        role.getName(), failures, threshold);

    if (failures > threshold) {
      throw new TriggerClusterTeardownException(
        SliderExitCodes.EXIT_DEPLOYMENT_FAILED,
          FinalApplicationStatus.FAILED, ErrorStrings.E_UNSTABLE_CLUSTER +
        " - failed with component %s failed 'recently' %d times (%d in startup);" +
        " threshold is %d - last failure: %s",
          role.getName(),
        role.getFailed(),
        role.getStartFailed(),
          threshold,
        role.getFailureMessage());
    }
  }

  /**
   * Get the failure threshold for a specific role, falling back to
   * the global one if not
   * @param roleStatus role
   * @return the threshold for failures
   */
  private int getFailureThresholdForRole(RoleStatus roleStatus) {
    ConfTreeOperations resources =
        instanceDefinition.getResourceOperations();
    return resources.getComponentOptInt(roleStatus.getName(),
        ResourceKeys.CONTAINER_FAILURE_THRESHOLD,
        failureThreshold);
  }

  /**
   * Get the node failure threshold for a specific role, falling back to
   * the global one if not
   * @param roleName role name
   * @return the threshold for failures
   */
  private int getNodeFailureThresholdForRole(String roleName) {
    ConfTreeOperations resources =
        instanceDefinition.getResourceOperations();
    return resources.getComponentOptInt(roleName,
                                        ResourceKeys.NODE_FAILURE_THRESHOLD,
                                        nodeFailureThreshold);
  }

  /**
   * Reset the "recent" failure counts of all roles
   */
  public void resetFailureCounts() {
    for (RoleStatus roleStatus : getRoleStatusMap().values()) {
      long failed = roleStatus.resetFailedRecently();
      log.info("Resetting failure count of {}; was {}",
               roleStatus.getName(),
          failed);
    }
    roleHistory.resetFailedRecently();
  }

  /**
   * Escalate operation as triggered by external timer.
   * @return a (usually empty) list of cancel/request operations.
   */
  public List<AbstractRMOperation> escalateOutstandingRequests() {
    return roleHistory.escalateOutstandingRequests();
  }
  
  /**
   * Look at the allocation status of one role, and trigger add/release
   * actions if the number of desired role instances doesn't equal 
   * (actual + pending).
   * <p>
   * MUST be executed from within a synchronized method
   * <p>
   * @param role role
   * @return a list of operations
   * @throws SliderInternalStateException if the operation reveals that
   * the internal state of the application is inconsistent.
   */
  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private List<AbstractRMOperation> reviewOneRole(RoleStatus role)
      throws SliderInternalStateException, TriggerClusterTeardownException {
    List<AbstractRMOperation> operations = new ArrayList<>();
    int delta;
    int expected;
    String name = role.getName();
    synchronized (role) {
      delta = role.getDelta();
      expected = role.getDesired();
    }

    log.info("Reviewing {} : expected {}", role, expected);
    checkFailureThreshold(role);

    if (expected < 0 ) {
      // negative value: fail
      throw new TriggerClusterTeardownException(
          SliderExitCodes.EXIT_DEPLOYMENT_FAILED,
          FinalApplicationStatus.FAILED,
          "Negative component count of %d desired for component %s",
          expected, role);
    }

    if (delta > 0) {
      log.info("{}: Asking for {} more nodes(s) for a total of {} ", name,
               delta, expected);
      //more workers needed than we have -ask for more
      for (int i = 0; i < delta; i++) {
        Resource capability = recordFactory.newResource();
        AMRMClient.ContainerRequest containerAsk =
          buildContainerResourceAndRequest(role, capability);
        log.info("Container ask is {} and label = {}", containerAsk,
            containerAsk.getNodeLabelExpression());
        int askMemory = containerAsk.getCapability().getMemory();
        if (askMemory > this.containerMaxMemory) {
          log.warn("Memory requested: {} > max of {}", askMemory, containerMaxMemory);
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

      // how many requests are outstanding
      int outstandingRequests = role.getRequested();
      if (outstandingRequests > 0) {
        // outstanding requests.
        int toCancel = Math.min(outstandingRequests, excess);

        // Delegate to Role History

        List<AbstractRMOperation> cancellations = roleHistory.cancelRequestsForRole(role, toCancel);
        log.info("Found {} outstanding requests to cancel", cancellations.size());
        operations.addAll(cancellations);
        if (toCancel != cancellations.size()) {
          log.error("Tracking of outstanding requests is not in sync with the summary statistics:" +
              " expected to be able to cancel {} requests, but got {}",
              toCancel, cancellations.size());
        }

        role.cancel(toCancel);
        excess -= toCancel;
        assert excess >= 0 : "Attempted to cancel too many requests";
        log.info("Submitted {} cancellations, leaving {} to release",
            toCancel, excess);
        if (excess == 0) {
          log.info("After cancelling requests, application is now at desired size");
        }
      }


      // after the cancellation there may be no excess
      if (excess > 0) {

        // there's an excess, so more to cancel
        // get the nodes to release
        int roleId = role.getKey();

        // enum all active nodes that aren't being released
        List<RoleInstance> containersToRelease = enumNodesWithRoleId(roleId, true);
        if (containersToRelease.isEmpty()) {
          log.info("No containers for component {}", roleId);
        }

        // cut all release-in-progress nodes
        ListIterator<RoleInstance> li = containersToRelease.listIterator();
        while (li.hasNext()) {
          RoleInstance next = li.next();
          if (next.released) {
            li.remove();
          }
        }

        // warn if the desired state can't be reached
        int numberAvailableForRelease = containersToRelease.size();
        if (numberAvailableForRelease < excess) {
          log.warn("Not enough containers to release, have {} and need {} more",
              numberAvailableForRelease,
              excess - numberAvailableForRelease);
        }

        // ask the release selector to sort the targets
        containersToRelease =  containerReleaseSelector.sortCandidates(
            roleId,
            containersToRelease,
            excess);

        //crop to the excess

        List<RoleInstance> finalCandidates = (excess < numberAvailableForRelease) 
            ? containersToRelease.subList(0, excess)
            : containersToRelease;


        // then build up a release operation, logging each container as released
        for (RoleInstance possible : finalCandidates) {
          log.info("Targeting for release: {}", possible);
          containerReleaseSubmitted(possible.container);
          operations.add(new ContainerReleaseOperation(possible.getId()));
        }
      }

    }

    // list of operations to execute
    return operations;
  }

  /**
   * Releases a container based on container id
   * @param containerId
   * @return
   * @throws SliderInternalStateException
   */
  public List<AbstractRMOperation> releaseContainer(ContainerId containerId)
      throws SliderInternalStateException {
    List<AbstractRMOperation> operations = new ArrayList<AbstractRMOperation>();
    List<RoleInstance> activeRoleInstances = cloneOwnedContainerList();
    for (RoleInstance role : activeRoleInstances) {
      if (role.container.getId().equals(containerId)) {
        containerReleaseSubmitted(role.container);
        operations.add(new ContainerReleaseOperation(role.getId()));
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
    Collection<RoleInstance> targets = cloneOwnedContainerList();
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

    Collection<RoleInstance> targets = cloneOwnedContainerList();
    log.info("Releasing {} containers", targets.size());
    List<AbstractRMOperation> operations =
      new ArrayList<>(targets.size());
    for (RoleInstance instance : targets) {
      if (instance.roleId == SliderKeys.ROLE_AM_PRIORITY_INDEX) {
        // don't worry about the AM
        continue;
      }
      Container possible = instance.container;
      ContainerId id = possible.getId();
      if (!instance.released) {
        String url = getLogsURLForContainer(possible);
        log.info("Releasing container. Log: " + url);
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
   * a list of operations to perform
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
      //get the role
      final ContainerId cid = container.getId();
      final RoleStatus role = lookupRoleStatus(container);
      

      //dec requested count
      decrementRequestCount(role);

      //inc allocated count -this may need to be dropped in a moment,
      // but us needed to update the logic below
      final int allocated = role.incActual();
      final int desired = role.getDesired();

      final String roleName = role.getName();
      final ContainerAllocation allocation =
          roleHistory.onContainerAllocated(container, desired, allocated);
      final ContainerAllocationOutcome outcome = allocation.outcome;

      // cancel an allocation request which granted this, so as to avoid repeated
      // requests
      if (allocation.origin != null && allocation.origin.getIssuedRequest() != null) {
        releaseOperations.add(allocation.origin.createCancelOperation());
      } else {
        // there's a request, but no idea what to cancel.
        // rather than try to recover from it inelegantly, (and cause more confusion),
        // log the event, but otherwise continue
        log.warn("Unexpected allocation of container "
            + SliderUtils.containerToString(container));
      }

      //look for condition where we get more back than we asked
      if (allocated > desired) {
        log.info("Discarding surplus {} container {} on {}", roleName,  cid,
            containerHostInfo);
        releaseOperations.add(new ContainerReleaseOperation(cid));
        //register as a surplus node
        surplusNodes.add(cid);
        surplusContainers.inc();
        //and, as we aren't binding it to role, dec that role's actual count
        role.decActual();
      } else {

        // Allocation being accepted -so decrement the number of outstanding requests
        decOutstandingContainerRequests();

        log.info("Assigning role {} to container" +
                 " {}," +
                 " on {}:{},",
                 roleName,
                 cid,
                 container.getNodeId().getHost(),
                 container.getNodeId().getPort()
                );

        assignments.add(new ContainerAssignment(container, role, outcome));
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
  private boolean rebuildModelFromRestart(List<Container> liveContainers)
      throws BadClusterStateException {
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
  private void addRestartedContainer(Container container)
      throws BadClusterStateException {
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
    instance.state = STATE_LIVE;
    instance.appVersion = SliderKeys.APP_VERSION_UNKNOWN;
    putOwnedContainer(cid, instance);
    //role history gets told
    roleHistory.onContainerAssigned(container);
    // pretend the container has just had its start actions submitted
    containerStartSubmitted(container, instance);
    // now pretend it has just started
    innerOnNodeManagerContainerStarted(cid);
  }
}
