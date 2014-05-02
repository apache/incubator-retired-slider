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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.providers.ProviderRole;
import org.apache.slider.server.avro.RoleHistoryHeader;
import org.apache.slider.server.avro.RoleHistoryWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * The Role History.
 * 
 * Synchronization policy: all public operations are synchronized.
 * Protected methods are in place for testing -no guarantees are made.
 * 
 * Inner classes have no synchronization guarantees; they should be manipulated 
 * in these classes and not externally.
 * 
 * Note that as well as some methods marked visible for testing, there
 * is the option for the time generator method, {@link #now()} to
 * be overridden so that a repeatable time series can be used.
 * 
 */
public class RoleHistory {
  protected static final Logger log =
    LoggerFactory.getLogger(RoleHistory.class);
  private final List<ProviderRole> providerRoles;
  private final Map<String, ProviderRole> providerRoleMap =
    new HashMap<String, ProviderRole>();
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

  public RoleHistory(List<ProviderRole> providerRoles) throws
                                                       BadConfigException {
    this.providerRoles = providerRoles;
    roleSize = providerRoles.size();
    for (ProviderRole providerRole : providerRoles) {
      providerRoleMap.put(providerRole.name, providerRole);
    }
    reset();
  }

  /**
   * Reset the variables -this does not adjust the fixed attributes
   * of the history
   */
  protected synchronized void reset() throws BadConfigException {

    nodemap = new NodeMap(roleSize);
    resetAvailableNodeLists();

    resetAvailableNodeLists();
    outstandingRequests = new OutstandingRequestTracker();
    Map<Integer, RoleStatus> roleStats = new HashMap<Integer, RoleStatus>();


    for (ProviderRole providerRole : providerRoles) {
      addProviderRole(roleStats, providerRole);
    }
  }

  
  private void addProviderRole(Map<Integer, RoleStatus> roleStats,
                               ProviderRole providerRole)
    throws ArrayIndexOutOfBoundsException, BadConfigException {
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
    Map<Integer, RoleStatus> roleStats = new HashMap<Integer, RoleStatus>();


    for (ProviderRole role : providerRoles) {
      roleStats.put(role.id, new RoleStatus(role));
    }

    addProviderRole(roleStats, providerRole);
  }

  /**
   * Clear the lists of available nodes
   */
  private synchronized void resetAvailableNodeLists() {
    availableNodes = new HashMap<Integer, LinkedList<NodeInstance>>(roleSize);
  }

  /**
   * Reset the variables -this does not adjust the fixed attributes
   * of the history.
   * This intended for use by the RoleWriter logic.
   */
  public synchronized void prepareForReading(RoleHistoryHeader header) throws
                                                                       IOException,
                                                                       BadConfigException {
    reset();

    int roleCountInSource = header.getRoles();
    if (roleCountInSource != roleSize) {
      throw new IOException("Number of roles in source " + roleCountInSource
                                + " does not match the expected number of " +
                                roleSize);
    }
    //record when the data was loaded
    setThawedDataTime(header.getSaved());
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
   * @param nodeAddr node address
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
   * Garbage collect the structure -this will dropp
   * all nodes that have been inactive since the (relative) age
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
    log.info("Role history bootstrapped");
  }

  /**
   * Handle the thaw process <i>after the history has been rebuilt</i>,
   * and after any gc/purge
   */
  @VisibleForTesting
  public synchronized boolean onThaw() throws BadConfigException {
    assert filesystem != null;
    assert historyPath != null;
    boolean thawSuccessful = false;
    //load in files from data dir
    Path loaded = null;
    try {
      loaded = historyWriter.loadFromHistoryDir(filesystem, historyPath, this);
    } catch (IOException e) {
      log.warn("Exception trying to load history from {}", historyPath, e);
    }
    if (loaded != null) {
      thawSuccessful = true;
      log.info("loaded history from {}", loaded);
      // delete any old entries
      try {
        int count = historyWriter.purgeOlderHistoryEntries(filesystem, loaded);
        log.debug("Deleted {} old history entries", count);
      } catch (IOException e) {
        log.info("Ignoring exception raised while trying to delete old entries",
                 e);
      }

      //thaw is then completed
      buildAvailableNodeLists();
    } else {
      //fallback to bootstrap procedure
      onBootstrap();
    }
    return thawSuccessful;
  }


  /**
   * (After the thaw), rebuild the availability datastructures
   */
  @VisibleForTesting
  public synchronized void buildAvailableNodeLists() {
    resetAvailableNodeLists();
    // build the list of available nodes
    for (Map.Entry<String, NodeInstance> entry : nodemap
      .entrySet()) {
      NodeInstance ni = entry.getValue();
      for (int i = 0; i < roleSize; i++) {
        NodeEntry nodeEntry = ni.get(i);
        if (nodeEntry != null && nodeEntry.isAvailable()) {
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
   * @return list
   */
  private LinkedList<NodeInstance> getNodesForRoleId(int id) {
    return availableNodes.get(id);
  }
  
  /**
   * Get the nodes for an ID -may be null
   * @param id role ID
   * @return list
   */
  private LinkedList<NodeInstance> getOrCreateNodesForRoleId(int id) {
    LinkedList<NodeInstance> instances = availableNodes.get(id);
    if (instances==null) {
      instances = new LinkedList<NodeInstance>();
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
      Collections.sort(nodesForRoleId, new NodeInstance.newerThan(role));
    }
  }

  public synchronized void onAMRestart() {
    //TODO once AM restart is implemented and we know what to expect
  }

  /**
   * Find a node for use
   * @param role role
   * @return the instance, or null for none
   */
  @VisibleForTesting
  public synchronized NodeInstance findNodeForNewInstance(RoleStatus role) {
    if (role.getNoDataLocality()) {
      return null;
    }
    int roleKey = role.getKey();
    NodeInstance nodeInstance = null;
    
    List<NodeInstance> targets = getNodesForRoleId(roleKey);
    while (targets != null && !targets.isEmpty() && nodeInstance == null) {
      NodeInstance head = targets.remove(0);
      if (head.getActiveRoleInstances(roleKey) == 0) {
        nodeInstance = head;
      }
    }
    return nodeInstance;
  }

  /**
   * Request an instance on a given node.
   * An outstanding request is created & tracked, with the 
   * relevant node entry for that role updated.
   *
   * The role status entries will also be tracked
   * 
   * Returns the request that is now being tracked.
   * If the node instance is not null, it's details about the role is incremented
   *
   *
   * @param node node to target or null for "any"
   * @param role role to request
   * @return the container priority
   */
  public synchronized AMRMClient.ContainerRequest requestInstanceOnNode(
    NodeInstance node, RoleStatus role, Resource resource) {
    OutstandingRequest outstanding = outstandingRequests.addRequest(node, role.getKey());
    return outstanding.buildContainerRequest(resource, role, now());
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
    return requestInstanceOnNode(node, role, resource);
  }


  /**
   * Find a list of node for release; algorithm may make its own
   * decisions on which to release.
   * @param role role index
   * @param count number of nodes to release
   * @return a possibly empty list of nodes.
   */
  public synchronized List<NodeInstance> findNodesForRelease(int role, int count) {
    return nodemap.findNodesForRelease(role, count);
  }
 
  /**
   * Get the node entry of a container
   * @param container container to look up
   * @return the entry
   */
  public NodeEntry getOrCreateNodeEntry(Container container) {
    NodeInstance node = getOrCreateNodeInstance(container);
    return node.getOrCreate(ContainerPriority.extractRole(container));
  }

  /**
   * Get the node instance of a container -always returns something
   * @param container container to look up
   * @return a (possibly new) node instance
   */
  public synchronized NodeInstance getOrCreateNodeInstance(Container container) {
    String hostname = RoleHistoryUtils.hostnameOf(container);
    return nodemap.getOrCreate(hostname);
  }

  /**
   * Get the node instance of a an address if defined
   * @param addr address
   * @return a node instance or null
   */
  public synchronized NodeInstance getExistingNodeInstance(String hostname) {
    return nodemap.get(hostname);
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
      new ArrayList<Container>(allocatedContainers.size());
    List<Container> unrequested =
      new ArrayList<Container>(allocatedContainers.size());
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
   * @return true if an entry was found and dropped
   */
  public synchronized boolean onContainerAllocated(Container container, int desiredCount, int actualCount) {
    int role = ContainerPriority.extractRole(container);
    String hostname = RoleHistoryUtils.hostnameOf(container);
    boolean requestFound =
      outstandingRequests.onContainerAllocated(role, hostname);
    if (desiredCount <= actualCount) {
      //cancel the nodes
      List<NodeInstance>
        hosts = outstandingRequests.cancelOutstandingRequests(role);
      if (!hosts.isEmpty()) {
        //add the list
        getOrCreateNodesForRoleId(role).addAll(hosts);
        sortAvailableNodeList(role);
      }
    }
    return requestFound;
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
   * @param container
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
    return markContainerFinished(container, false, true);
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
    return markContainerFinished(container, true, false);
  }

  /**
   * App state notified of a container completed -but as
   * it wasn't being released it is marked as failed
   *
   * @param container completed container
   * @param shortLived was the container short lived?
   * @return true if the node is considered available for work
   */
  public boolean onFailedContainer(Container container, boolean shortLived) {
    return markContainerFinished(container, false, shortLived);
  }

  /**
   * Mark a container finished; if it was released then that is treated
   * differently. history is touch()ed
   *
   *
   * @param container completed container
   * @param wasReleased was the container released?
   * @param shortLived was the container short lived?
   * @return true if the node was queued
   */
  protected synchronized boolean markContainerFinished(Container container,
                                                       boolean wasReleased,
                                                       boolean shortLived) {
    NodeEntry nodeEntry = getOrCreateNodeEntry(container);
    boolean available;
    if (shortLived) {
      nodeEntry.onStartFailed();
      available = false;
    } else {
      available = nodeEntry.containerCompleted(wasReleased);
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
               + " " + SliderUtils.joinWithInnerSeparator(", ", instances));
    }

    log.info("Nodes in Cluster: {}", getClusterSize());
    for (NodeInstance node : nodemap.values()) {
      log.info(node.toFullString());
    }
  }

  /**
   * Get a clone of the available list
   * @param role role index
   * @return a clone of the list
   */
  @VisibleForTesting
  public List<NodeInstance> cloneAvailableList(int role) {
    return new LinkedList<NodeInstance>(getOrCreateNodesForRoleId(role));
  }

  /**
   * Get a snapshot of the outstanding request list
   * @return a list of the requests outstanding at the time of requesting
   */
  @VisibleForTesting
  public synchronized List<OutstandingRequest> getOutstandingRequestList() {
    return outstandingRequests.listOutstandingRequests();
  }


}
