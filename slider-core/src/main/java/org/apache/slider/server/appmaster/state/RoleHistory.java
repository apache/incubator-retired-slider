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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.avro.LoadedRoleHistory;
import org.apache.slider.server.avro.NodeEntryRecord;
import org.apache.slider.server.avro.RoleHistoryHeader;
import org.apache.slider.server.avro.RoleHistoryWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The Role History.
 * <p>
 * Synchronization policy: all public operations are synchronized.
 * Protected methods are in place for testing -no guarantees are made.
 * <p>
 * Inner classes have no synchronization guarantees; they should be manipulated 
 * in these classes and not externally.
 * <p>
 * Note that as well as some methods marked visible for testing, there
 * is the option for the time generator method, {@link #now()} to
 * be overridden so that a repeatable time series can be used.
 * 
 */
public class RoleHistory {
  protected static final Logger log =
    LoggerFactory.getLogger(RoleHistory.class);
  private final List<ProviderRole> providerRoles;
  private long startTime;
  /**
   * Time when saved
   */
  private long saveTime;
  /**
   * If the history was loaded, the time at which the history was saved
   * 
   */
  private long thawedDataTime;
  
  private NodeMap nodemap;
  private int roleSize;
  private boolean dirty;
  private FileSystem filesystem;
  private Path historyPath;
  private RoleHistoryWriter historyWriter = new RoleHistoryWriter();

  private OutstandingRequestTracker outstandingRequests =
    new OutstandingRequestTracker();

  /**
   * For each role, lists nodes that are available for data-local allocation,
   ordered by more recently released - To accelerate node selection
   */
  private Map<Integer, LinkedList<NodeInstance>> availableNodes;

  /**
   * Track the failed nodes. Currently used to make wiser decision of container
   * ask with/without locality. Has other potential uses as well.
   */
  private Set<String> failedNodes = new HashSet<>();


  public RoleHistory(List<ProviderRole> providerRoles) throws
                                                       BadConfigException {
    this.providerRoles = providerRoles;
    roleSize = providerRoles.size();
    reset();
  }

  /**
   * Reset the variables -this does not adjust the fixed attributes
   * of the history
   */
  protected synchronized void reset() throws BadConfigException {

    nodemap = new NodeMap(roleSize);
    resetAvailableNodeLists();

    outstandingRequests = new OutstandingRequestTracker();
    
    Map<Integer, RoleStatus> roleStats = new HashMap<>();
    for (ProviderRole providerRole : providerRoles) {
      checkProviderRole(roleStats, providerRole);
    }
  }

  /**
   * safety check: make sure the provider role is unique amongst
   * the role stats...which is extended with the new role
   * @param roleStats role stats
   * @param providerRole role
   * @throws ArrayIndexOutOfBoundsException
   * @throws BadConfigException
   */
  protected void checkProviderRole(Map<Integer, RoleStatus> roleStats,
      ProviderRole providerRole)
    throws BadConfigException {
    int index = providerRole.id;
    if (index < 0) {
      throw new BadConfigException("Provider " + providerRole
                                               + " id is out of range");
    }
    if (roleStats.get(index) != null) {
      throw new BadConfigException(
        providerRole.toString() + " id duplicates that of " +
        roleStats.get(index));
    }
    roleStats.put(index, new RoleStatus(providerRole));
  }


  /**
   * Add a new provider role to the map
   * @param providerRole new provider role
   */
  public void addNewProviderRole(ProviderRole providerRole)
    throws BadConfigException {
    log.debug("Validating/adding new provider role to role history: {} ",
        providerRole);
    Map<Integer, RoleStatus> roleStats = new HashMap<>();

    for (ProviderRole role : providerRoles) {
      roleStats.put(role.id, new RoleStatus(role));
    }

    checkProviderRole(roleStats, providerRole);
    log.debug("Check successful; adding role");
    this.providerRoles.add(providerRole);
  }

  /**
   * Lookup a role by ID
   * @param roleId role Id
   * @return role or null if not found
   */
  public ProviderRole lookupRole(int roleId) {
    for (ProviderRole role : providerRoles) {
      if (role.id == roleId) {
        return role;
      }
    }
    return null;
  }

  /**
   * Clear the lists of available nodes
   */
  private synchronized void resetAvailableNodeLists() {
    availableNodes = new HashMap<>(roleSize);
  }

  /**
   * Reset the variables -this does not adjust the fixed attributes
   * of the history.
   * <p>
   * This intended for use by the RoleWriter logic.
   * @throws BadConfigException if there is a problem rebuilding the state
   */
  private void prepareForReading(RoleHistoryHeader header)
      throws BadConfigException {
    reset();

    int roleCountInSource = header.getRoles();
    if (roleCountInSource != roleSize) {
      log.warn("Number of roles in source {}"
              +" does not match the expected number of {}",
          roleCountInSource,
          roleSize);
    }
    //record when the data was loaded
    setThawedDataTime(header.getSaved());
  }

  /**
   * rebuild the placement history from the loaded role history
   * @param loadedRoleHistory loaded history
   * @return the number of entries discarded
   * @throws BadConfigException if there is a problem rebuilding the state
   */
  @VisibleForTesting
  public synchronized int rebuild(LoadedRoleHistory loadedRoleHistory) throws BadConfigException {
    RoleHistoryHeader header = loadedRoleHistory.getHeader();
    prepareForReading(header);
    int discarded = 0;
    Long saved = header.getSaved();
    for (NodeEntryRecord nodeEntryRecord : loadedRoleHistory.records) {
      Integer roleId = nodeEntryRecord.getRole();
      NodeEntry nodeEntry = new NodeEntry(roleId);
      nodeEntry.setLastUsed(nodeEntryRecord.getLastUsed());
      if (nodeEntryRecord.getActive()) {
        //if active at the time of save, make the last used time the save time
        nodeEntry.setLastUsed(saved);
      }
      String hostname = SliderUtils.sequenceToString(nodeEntryRecord.getHost());
      ProviderRole providerRole = lookupRole(roleId);
      if (providerRole == null) {
        // discarding entry
        log.info("Discarding history entry with unknown role: {} on host {}",
            roleId, hostname);
        discarded ++;
      } else {
        NodeInstance instance = getOrCreateNodeInstance(hostname);
        instance.set(roleId, nodeEntry);
      }
    }
    return discarded;
  }


  public synchronized long getStartTime() {
    return startTime;
  }

  public synchronized long getSaveTime() {
    return saveTime;
  }

  public long getThawedDataTime() {
    return thawedDataTime;
  }

  public void setThawedDataTime(long thawedDataTime) {
    this.thawedDataTime = thawedDataTime;
  }

  public synchronized int getRoleSize() {
    return roleSize;
  }

  /**
   * Get the total size of the cluster -the number of NodeInstances
   * @return a count
   */
  public synchronized int getClusterSize() {
    return nodemap.size();
  }

  public synchronized boolean isDirty() {
    return dirty;
  }

  public synchronized void setDirty(boolean dirty) {
    this.dirty = dirty;
  }

  /**
   * Tell the history that it has been saved; marks itself as clean
   * @param timestamp timestamp -updates the savetime field
   */
  public synchronized void saved(long timestamp) {
    dirty = false;
    saveTime = timestamp;
  }

  /**
   * Get a clone of the nodemap.
   * The instances inside are not cloned
   * @return the map
   */
  public synchronized NodeMap cloneNodemap() {
    return (NodeMap) nodemap.clone();
  }

  /**
   * Get the node instance for the specific node -creating it if needed
   * @param hostname node address
   * @return the instance
   */
  public synchronized NodeInstance getOrCreateNodeInstance(String hostname) {
    //convert to a string
    return nodemap.getOrCreate(hostname);
  }

  /**
   * Insert a list of nodes into the map; overwrite any with that name.
   * This is a bulk operation for testing.
   * Important: this does not update the available node lists, these
   * must be rebuilt afterwards.
   * @param nodes collection of nodes.
   */
  @VisibleForTesting
  public synchronized void insert(Collection<NodeInstance> nodes) {
    nodemap.insert(nodes);
  }
  
  /**
   * Get current time. overrideable for test subclasses
   * @return current time in millis
   */
  protected long now() {
    return System.currentTimeMillis();
  }

  /**
   * Garbage collect the structure -this will drop
   * all nodes that have been inactive since the (relative) age.
   * This will drop the failure counts of the nodes too, so it will
   * lose information that matters.
   * @param age relative age
   */
  public void gc(long age) {
    long absoluteTime = now() - age;
    purgeUnusedEntries(absoluteTime);
  }

  /**
   * Mark ourselves as dirty
   */
  public void touch() {
    setDirty(true);
    try {
      saveHistoryIfDirty();
    } catch (IOException e) {
      log.warn("Failed to save history file ", e);
    }
  }

  /**
   * purge the history of
   * all nodes that have been inactive since the absolute time
   * @param absoluteTime time
   */
  public synchronized void purgeUnusedEntries(long absoluteTime) {
    nodemap.purgeUnusedEntries(absoluteTime);
  }

  /**
   * reset the failed recently counters
   */
  public synchronized void resetFailedRecently() {
    log.info("Resetting failure history");
    nodemap.resetFailedRecently();
  }

  /**
   * Get the path used for history files
   * @return the directory used for history files
   */
  public Path getHistoryPath() {
    return historyPath;
  }

  /**
   * Save the history to its location using the timestamp as part of
   * the filename. The saveTime and dirty fields are updated
   * @param time timestamp timestamp to use as the save time
   * @return the path saved to
   * @throws IOException IO problems
   */
  @VisibleForTesting
  public synchronized Path saveHistory(long time) throws IOException {
    Path filename = historyWriter.createHistoryFilename(historyPath, time);
    historyWriter.write(filesystem, filename, true, this, time);
    saved(time);
    return filename;
  }

  /**
   * Save the history with the current timestamp if it is dirty;
   * return the path saved to if this is the case
   * @return the path or null if the history was not saved
   * @throws IOException failed to save for some reason
   */
  public synchronized Path saveHistoryIfDirty() throws IOException {
    if (isDirty()) {
      long time = now();
      return saveHistory(time);
    } else {
      return null;
    }
  } 

  /**
   * Start up
   * @param fs filesystem 
   * @param historyDir path in FS for history
   * @return true if the history was thawed
   */
  public boolean onStart(FileSystem fs, Path historyDir) throws
                                                         BadConfigException {
    assert filesystem == null;
    filesystem = fs;
    historyPath = historyDir;
    startTime = now();
    //assume the history is being thawed; this will downgrade as appropriate
    return onThaw();
    }
  
  /**
   * Handler for bootstrap event
   */
  public void onBootstrap() {
    log.debug("Role history bootstrapped");
  }

  /**
   * Handle the start process <i>after the history has been rebuilt</i>,
   * and after any gc/purge
   */
  public synchronized boolean onThaw() throws BadConfigException {
    assert filesystem != null;
    assert historyPath != null;
    boolean thawSuccessful = false;
    //load in files from data dir

    LoadedRoleHistory loadedRoleHistory = null;
    try {
      loadedRoleHistory = historyWriter.loadFromHistoryDir(filesystem, historyPath);
    } catch (IOException e) {
      log.warn("Exception trying to load history from {}", historyPath, e);
    }
    if (loadedRoleHistory != null) {
      rebuild(loadedRoleHistory);
      thawSuccessful = true;
      Path loadPath = loadedRoleHistory.getPath();;
      log.debug("loaded history from {}", loadPath);
      // delete any old entries
      try {
        int count = historyWriter.purgeOlderHistoryEntries(filesystem, loadPath);
        log.debug("Deleted {} old history entries", count);
      } catch (IOException e) {
        log.info("Ignoring exception raised while trying to delete old entries",
                 e);
      }

      //start is then completed
      buildAvailableNodeLists();
    } else {
      //fallback to bootstrap procedure
      onBootstrap();
    }
    return thawSuccessful;
  }


  /**
   * (After the start), rebuild the availability data structures
   */
  @VisibleForTesting
  public synchronized void buildAvailableNodeLists() {
    resetAvailableNodeLists();
    // build the list of available nodes
    for (Map.Entry<String, NodeInstance> entry : nodemap.entrySet()) {
      NodeInstance ni = entry.getValue();
      for (int i = 0; i < roleSize; i++) {
        NodeEntry nodeEntry = ni.get(i);
        if (nodeEntry != null && nodeEntry.isAvailable()) {
          log.debug("Adding {} for role {}", ni, i);
          getOrCreateNodesForRoleId(i).add(ni);
        }
      }
    }
    // sort the resulting arrays
    for (int i = 0; i < roleSize; i++) {
      sortAvailableNodeList(i);
    }
  }

  /**
   * Get the nodes for an ID -may be null
   * @param id role ID
   * @return potentially null list
   */
  @VisibleForTesting
  public List<NodeInstance> getNodesForRoleId(int id) {
    return availableNodes.get(id);
  }
  
  /**
   * Get the nodes for an ID -may be null
   * @param id role ID
   * @return list
   */
  private LinkedList<NodeInstance> getOrCreateNodesForRoleId(int id) {
    LinkedList<NodeInstance> instances = availableNodes.get(id);
    if (instances == null) {
      instances = new LinkedList<>();
      availableNodes.put(id, instances);
    }
    return instances;
  }
  
  /**
   * Sort an available node list
   * @param role role to sort
   */
  private void sortAvailableNodeList(int role) {
    List<NodeInstance> nodesForRoleId = getNodesForRoleId(role);
    if (nodesForRoleId != null) {
      Collections.sort(nodesForRoleId, new NodeInstance.Preferred(role));
    }
  }

  /**
   * Find a node for use
   * @param role role
   * @return the instance, or null for none
   */
  @VisibleForTesting
  public synchronized NodeInstance findNodeForNewInstance(RoleStatus role) {
    if (!role.isPlacementDesired()) {
      // no data locality policy
      return null;
    }
    int roleId = role.getKey();
    boolean strictPlacement = role.isStrictPlacement();
    NodeInstance nodeInstance = null;
    // Get the list of possible targets.
    // This is a live list: changes here are preserved
    List<NodeInstance> targets = getNodesForRoleId(roleId);
    if (targets == null) {
      // nothing to allocate on
      return null;
    }

    int cnt = targets.size();
    log.debug("There are {} node(s) to consider for {}", cnt, role.getName());
    for (int i = 0; i < cnt  && nodeInstance == null; i++) {
      NodeInstance candidate = targets.get(i);
      if (candidate.getActiveRoleInstances(roleId) == 0) {
        // no active instances: check failure statistics
        if (strictPlacement || !candidate.exceedsFailureThreshold(role)) {
          targets.remove(i);
          // exit criteria for loop is now met
          nodeInstance = candidate;
        } else {
          // too many failures for this node
          log.info("Recent node failures is higher than threshold {}. Not requesting host {}",
              role.getNodeFailureThreshold(), candidate.hostname);
        }
      }
    }

    if (nodeInstance == null) {
      log.info("No node found for {}", role.getName());
    }
    return nodeInstance;
  }

  /**
   * Request an instance on a given node.
   * An outstanding request is created & tracked, with the 
   * relevant node entry for that role updated.
   *<p>
   * The role status entries will also be tracked
   * <p>
   * Returns the request that is now being tracked.
   * If the node instance is not null, it's details about the role is incremented
   *
   *
   * @param node node to target or null for "any"
   * @param role role to request
   * @param labelExpression label to satisfy
   * @return the container priority
   */
  public synchronized AMRMClient.ContainerRequest requestInstanceOnNode(
    NodeInstance node, RoleStatus role, Resource resource, String labelExpression) {
    OutstandingRequest outstanding = outstandingRequests.newRequest(node, role.getKey());
    return outstanding.buildContainerRequest(resource, role, now(), labelExpression);
  }

  /**
   * Find a node for a role and request an instance on that (or a location-less
   * instance) with a label expression
   * @param role role status
   * @param resource resource capabilities
   * @param labelExpression label to satisfy
   * @return a request ready to go
   */
  public synchronized AMRMClient.ContainerRequest requestNode(RoleStatus role,
                                                              Resource resource,
                                                              String labelExpression) {
    NodeInstance node = findNodeForNewInstance(role);
    return requestInstanceOnNode(node, role, resource, labelExpression);
  }

  /**
   * Find a node for a role and request an instance on that (or a location-less
   * instance)
   * @param role role status
   * @param resource resource capabilities
   * @return a request ready to go
   */
  public synchronized AMRMClient.ContainerRequest requestNode(RoleStatus role,
                                                              Resource resource) {
    NodeInstance node = findNodeForNewInstance(role);
    return requestInstanceOnNode(node, role, resource, null);
  }

  /**
   * Get the list of active nodes ... walks the node  map so 
   * is O(nodes)
   * @param role role index
   * @return a possibly empty list of nodes with an instance of that node
   */
  public synchronized List<NodeInstance> listActiveNodes(int role) {
    return nodemap.listActiveNodes(role);
  }
  
  /**
   * Get the node entry of a container
   * @param container container to look up
   * @return the entry
   * @throws RuntimeException if the container has no hostname
   */
  public NodeEntry getOrCreateNodeEntry(Container container) {
    NodeInstance node = getOrCreateNodeInstance(container);
    return node.getOrCreate(ContainerPriority.extractRole(container));
  }

  /**
   * Get the node instance of a container -always returns something
   * @param container container to look up
   * @return a (possibly new) node instance
   * @throws RuntimeException if the container has no hostname
   */
  public synchronized NodeInstance getOrCreateNodeInstance(Container container) {
    String hostname = RoleHistoryUtils.hostnameOf(container);
    return nodemap.getOrCreate(hostname);
  }

  /**
   * Get the node instance of a host if defined
   * @param hostname hostname to look up
   * @return a node instance or null
   * @throws RuntimeException if the container has no hostname
   */
  public synchronized NodeInstance getExistingNodeInstance(String hostname) {
    return nodemap.get(hostname);
  }

  /**
   * Get the node instance of a container <i>if there's an entry in the history</i>
   * @param container container to look up
   * @return a node instance or null
   * @throws RuntimeException if the container has no hostname
   */
  public synchronized NodeInstance getExistingNodeInstance(Container container) {
    return nodemap.get(RoleHistoryUtils.hostnameOf(container));
  }

  /**
   * Perform any pre-allocation operations on the list of allocated containers
   * based on knowledge of system state. 
   * Currently this places requested hosts ahead of unrequested ones.
   * @param allocatedContainers list of allocated containers
   * @return list of containers potentially reordered
   */
  public synchronized List<Container> prepareAllocationList(List<Container> allocatedContainers) {
    
    //partition into requested and unrequested
    List<Container> requested =
      new ArrayList<>(allocatedContainers.size());
    List<Container> unrequested =
      new ArrayList<>(allocatedContainers.size());
    outstandingRequests.partitionRequests(this, allocatedContainers, requested, unrequested);

    //give the unrequested ones lower priority
    requested.addAll(unrequested);
    return requested;
  }
  
  /**
   * A container has been allocated on a node -update the data structures
   * @param container container
   * @param desiredCount desired #of instances
   * @param actualCount current count of instances
   * @return The allocation outcome
   */
  public synchronized ContainerAllocation onContainerAllocated(Container container,
      int desiredCount,
      int actualCount) {
    int role = ContainerPriority.extractRole(container);
    String hostname = RoleHistoryUtils.hostnameOf(container);
    List<NodeInstance> nodeInstances = getOrCreateNodesForRoleId(role);
    ContainerAllocation outcome =
        outstandingRequests.onContainerAllocated(role, hostname, container);
    if (desiredCount <= actualCount) {
      // all outstanding requests have been satisfied
      // clear all the lists, so returning nodes to the available set
      List<NodeInstance>
          hosts = outstandingRequests.resetOutstandingRequests(role);
      if (!hosts.isEmpty()) {
        //add the list
        log.info("Adding {} hosts for role {}", hosts.size(), role);
        nodeInstances.addAll(hosts);
        sortAvailableNodeList(role);
      }
    }
    return outcome;
  }

  /**
   * A container has been assigned to a role instance on a node -update the data structures
   * @param container container
   */
  public void onContainerAssigned(Container container) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    nodeEntry.onStarting();
  }

  /**
   * Event: a container start has been submitter
   * @param container container being started
   * @param instance instance bound to the container
   */
  public void onContainerStartSubmitted(Container container,
                                        RoleInstance instance) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    int role = ContainerPriority.extractRole(container);
    // any actions we want here
  }

  /**
   * Container start event
   * @param container container that just started
   */
  public void onContainerStarted(Container container) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    nodeEntry.onStartCompleted();
    touch();
  }

  /**
   * A container failed to start: update the node entry state
   * and return the container to the queue
   * @param container container that failed
   * @return true if the node was queued
   */
  public boolean onNodeManagerContainerStartFailed(Container container) {
    return markContainerFinished(container, false, true, ContainerOutcome.Failed);
  }

  /**
   * Update failedNodes and nodemap based on the node state
   * 
   * @param updatedNodes list of updated nodes
   */
  public synchronized void onNodesUpdated(List<NodeReport> updatedNodes) {
    for (NodeReport updatedNode : updatedNodes) {
      String hostname = updatedNode.getNodeId() == null 
                        ? null 
                        : updatedNode.getNodeId().getHost();
      if (hostname == null) {
        continue;
      }
      if (updatedNode.getNodeState() != null
          && updatedNode.getNodeState().isUnusable()) {
        failedNodes.add(hostname);
        nodemap.remove(hostname);
      } else {
        failedNodes.remove(hostname);
      }
    }
  }

  /**
   * A container release request was issued
   * @param container container submitted
   */
  public void onContainerReleaseSubmitted(Container container) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    nodeEntry.release();
  }

  /**
   * App state notified of a container completed 
   * @param container completed container
   * @return true if the node was queued
   */
  public boolean onReleaseCompleted(Container container) {
    return markContainerFinished(container, true, false, ContainerOutcome.Failed);
  }

  /**
   * App state notified of a container completed -but as
   * it wasn't being released it is marked as failed
   *
   * @param container completed container
   * @param shortLived was the container short lived?
   * @param outcome
   * @return true if the node is considered available for work
   */
  public boolean onFailedContainer(Container container,
      boolean shortLived,
      ContainerOutcome outcome) {
    return markContainerFinished(container, false, shortLived, outcome);
  }

  /**
   * Mark a container finished; if it was released then that is treated
   * differently. history is touch()ed
   *
   *
   * @param container completed container
   * @param wasReleased was the container released?
   * @param shortLived was the container short lived?
   * @param outcome
   * @return true if the node was queued
   */
  protected synchronized boolean markContainerFinished(Container container,
      boolean wasReleased,
      boolean shortLived,
      ContainerOutcome outcome) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    log.info("Finished container for node {}, released={}, shortlived={}",
        nodeEntry.rolePriority, wasReleased, shortLived);
    boolean available;
    if (shortLived) {
      nodeEntry.onStartFailed();
      available = false;
    } else {
      available = nodeEntry.containerCompleted(wasReleased, outcome);
      maybeQueueNodeForWork(container, nodeEntry, available);
    }
    touch();
    return available;
  }

  /**
   * If the node is marked as available; queue it for assignments.
   * Unsynced: expects caller to be in a sync block.
   * @param container completed container
   * @param nodeEntry node
   * @param available available flag
   * @return true if the node was queued
   */
  private boolean maybeQueueNodeForWork(Container container,
                                        NodeEntry nodeEntry,
                                        boolean available) {
    if (available) {
      //node is free
      nodeEntry.setLastUsed(now());
      NodeInstance ni = getOrCreateNodeInstance(container);
      int roleId = ContainerPriority.extractRole(container);
      log.debug("Node {} is now available for role id {}", ni, roleId);
      getOrCreateNodesForRoleId(roleId).addFirst(ni);
    }
    return available;
  }

  /**
   * Print the history to the log. This is for testing and diagnostics 
   */
  public synchronized void dump() {
    for (ProviderRole role : providerRoles) {
      log.info(role.toString());
      List<NodeInstance> instances =
        getOrCreateNodesForRoleId(role.id);
      log.info("  available: " + instances.size()
               + " " + SliderUtils.joinWithInnerSeparator(" ", instances));
    }

    log.info("Nodes in Cluster: {}", getClusterSize());
    for (NodeInstance node : nodemap.values()) {
      log.info(node.toFullString());
    }

    log.info("Failed nodes: {}",
        SliderUtils.joinWithInnerSeparator(" ", failedNodes));
  }

  /**
   * Build the mapping entry for persisting to the role history
   * @return a mapping object
   */
  public synchronized Map<CharSequence, Integer> buildMappingForHistoryFile() {
    Map<CharSequence, Integer> mapping = new HashMap<>(getRoleSize());
    for (ProviderRole role : providerRoles) {
      mapping.put(role.name, role.id);
    }
    return mapping;
  }

  /**
   * Get a clone of the available list
   * @param role role index
   * @return a clone of the list
   */
  @VisibleForTesting
  public List<NodeInstance> cloneAvailableList(int role) {
    return new LinkedList<>(getOrCreateNodesForRoleId(role));
  }

  /**
   * Get a snapshot of the outstanding placed request list
   * @return a list of the requests outstanding at the time of requesting
   */
  @VisibleForTesting
  public List<OutstandingRequest> listPlacedRequests() {
    return outstandingRequests.listPlacedRequests();
  }


  /**
   * Get a snapshot of the outstanding placed request list
   * @return a list of the requests outstanding at the time of requesting
   */
  @VisibleForTesting
  public List<OutstandingRequest> listOpenRequests() {
    return outstandingRequests.listOpenRequests();
  }

  /**
   * Get a clone of the failedNodes
   * 
   * @return the list
   */
  public List<String> cloneFailedNodes() {
    List<String> lst = new ArrayList<>();
    lst.addAll(failedNodes);
    return lst;
  }

  /**
   * Escalate operation as triggered by external timer.
   * @return a (usually empty) list of cancel/request operations.
   */
  public List<AbstractRMOperation> escalateOutstandingRequests() {
    long now = now();
    return outstandingRequests.escalateOutstandingRequests(now);
  }

  /**
   * Build the list of requests to cancel from the outstanding list.
   * @param role
   * @param toCancel
   * @return a list of cancellable operations.
   */
  public synchronized List<AbstractRMOperation> cancelRequestsForRole(RoleStatus role, int toCancel) {
    List<AbstractRMOperation> results = new ArrayList<>(toCancel);
    // first scan through the unplaced request list to find all of a role
    int roleId = role.getKey();
    List<OutstandingRequest> requests =
        outstandingRequests.extractOpenRequestsForRole(roleId, toCancel);

    // are there any left?
    int remaining = toCancel - requests.size();
    // ask for some placed nodes
    requests.addAll(outstandingRequests.extractPlacedRequestsForRole(roleId, remaining));

    // build cancellations
    for (OutstandingRequest request : requests) {
      results.add(request.createCancelOperation());
    }
    return results;
  }

}
