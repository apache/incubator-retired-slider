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
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.slider.api.ClusterDescription;
import org.apache.slider.core.conf.AggregateConf;
import org.apache.slider.core.conf.ConfTreeOperations;
import org.apache.slider.core.exceptions.NoSuchNodeException;
import org.apache.slider.core.registry.docstore.PublishedConfigSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The methods to offer state access to the providers
 */
public interface StateAccessForProviders {
  Map<Integer, RoleStatus> getRoleStatusMap();

  /**
   * Get the published configurations
   * @return the configuration set
   */
  PublishedConfigSet getPublishedConfigurations();

  Map<ContainerId, RoleInstance> getFailedNodes();

  Map<ContainerId, RoleInstance> getLiveNodes();

  /**
   * Get the current cluster description 
   * @return the actual state of the cluster
   */
  ClusterDescription getClusterStatus();

  /**
   * Get at the snapshot of the resource config
   * Changes here do not affect the application state.
   * @return the most recent settings
   */
  ConfTreeOperations getResourcesSnapshot();

  /**
   * Get at the snapshot of the appconf config
   * Changes here do not affect the application state.
   * @return the most recent settings
   */
  ConfTreeOperations getAppConfSnapshot();

  /**
   * Get at the snapshot of the internals config.
   * Changes here do not affect the application state.
   * @return the internals settings
   */

  ConfTreeOperations getInternalsSnapshot();

  /**
   * Flag set to indicate the application is live -this only happens
   * after the buildInstance operation
   */
  boolean isApplicationLive();

  long getSnapshotTime();

  AggregateConf getInstanceDefinitionSnapshot();

  /**
   * Look up a role from its key -or fail 
   *
   * @param key key to resolve
   * @return the status
   * @throws YarnRuntimeException on no match
   */
  RoleStatus lookupRoleStatus(int key);

  /**
   * Look up a role from its key -or fail 
   *
   * @param c container in a role
   * @return the status
   * @throws YarnRuntimeException on no match
   */
  RoleStatus lookupRoleStatus(Container c) throws YarnRuntimeException;

  /**
   * Look up a role from its key -or fail 
   *
   * @param name container in a role
   * @return the status
   * @throws YarnRuntimeException on no match
   */
  RoleStatus lookupRoleStatus(String name) throws YarnRuntimeException;

  /**
   * Clone a list of active containers
   * @return the active containers at the time
   * the call was made
   */
  List<RoleInstance> cloneActiveContainerList();

  /**
   * Get the number of active containers
   * @return the number of active containers the time the call was made
   */
  int getNumActiveContainers();

  /**
   * Get any active container with the given ID
   * @param id container Id
   * @return the active container or null if it is not found
   */
  RoleInstance getActiveContainer(ContainerId id);

  /**
   * Create a clone of the list of live cluster nodes.
   * @return the list of nodes, may be empty
   */
  List<RoleInstance> cloneLiveContainerInfoList();

  /**
   * Get the {@link RoleInstance} details on a container.
   * This is an O(n) operation
   * @param containerId the container ID
   * @return null if there is no such node
   * @throws NoSuchNodeException if the node cannot be found
   */
  RoleInstance getLiveInstanceByContainerID(String containerId)
    throws NoSuchNodeException;

  /**
   * Get the details on a list of instaces referred to by ID.
   * Unknown nodes are not returned
   * <i>Important: the order of the results are undefined</i>
   * @param containerIDs the containers
   * @return list of instances
   */
  List<RoleInstance> getLiveInstancesByContainerIDs(
    Collection<String> containerIDs);

  /**
   * Update the cluster description with anything interesting
   * @param providerStatus status from the provider for the cluster info section
   */
  void refreshClusterStatus();
}
