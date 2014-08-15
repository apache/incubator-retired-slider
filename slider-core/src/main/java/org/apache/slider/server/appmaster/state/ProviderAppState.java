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
import org.apache.slider.server.appmaster.web.rest.RestPaths;
import org.apache.slider.server.services.utility.PatternValidator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ProviderAppState implements StateAccessForProviders {


  private final Map<String, PublishedConfigSet> publishedConfigSets =
      new ConcurrentHashMap<String, PublishedConfigSet>(5);
  private static final PatternValidator validator = new PatternValidator(
      RestPaths.PUBLISHED_CONFIGURATION_SET_REGEXP);
  private String applicationName;

  private final AppState appState;

  public ProviderAppState(String applicationName, AppState appState) {
    this.appState = appState;
    this.applicationName = applicationName;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  @Override
  public String getApplicationName() {
    return applicationName;
  }

  @Override
  public PublishedConfigSet getPublishedSliderConfigurations() {
    return getOrCreatePublishedConfigSet(RestPaths.SLIDER_CONFIGSET);
  }

  @Override
  public PublishedConfigSet getPublishedConfigSet(String name) {
    return publishedConfigSets.get(name);
  }

  @Override
  public PublishedConfigSet getOrCreatePublishedConfigSet(String name) {
    PublishedConfigSet set = publishedConfigSets.get(name);
    if (set == null) {
      validator.validate(name);
      synchronized (publishedConfigSets) {
        // synchronized double check to ensure that there is never an overridden
        // config set created
        set = publishedConfigSets.get(name);
        if (set == null) {
          set = new PublishedConfigSet();
          publishedConfigSets.put(name, set);
        }
      }
    }
    return set;
  }

  @Override
  public List<String> listConfigSets() {

    synchronized (publishedConfigSets) {
      List<String> sets = new ArrayList<String>(publishedConfigSets.keySet());
      return sets;
    }
  }

  @Override
  public Map<Integer, RoleStatus> getRoleStatusMap() {
    return appState.getRoleStatusMap();
  }


  @Override
  public Map<ContainerId, RoleInstance> getFailedNodes() {
    return appState.getFailedNodes();
  }

  @Override
  public Map<ContainerId, RoleInstance> getLiveNodes() {
    return appState.getLiveNodes();
  }

  @Override
  public ClusterDescription getClusterStatus() {
    return appState.getClusterStatus();
  }

  @Override
  public ConfTreeOperations getResourcesSnapshot() {
    return appState.getResourcesSnapshot();
  }

  @Override
  public ConfTreeOperations getAppConfSnapshot() {
    return appState.getAppConfSnapshot();
  }

  @Override
  public ConfTreeOperations getInternalsSnapshot() {
    return appState.getInternalsSnapshot();
  }

  @Override
  public boolean isApplicationLive() {
    return appState.isApplicationLive();
  }

  @Override
  public long getSnapshotTime() {
    return appState.getSnapshotTime();
  }

  @Override
  public AggregateConf getInstanceDefinitionSnapshot() {
    return appState.getInstanceDefinitionSnapshot();
  }

  @Override
  public RoleStatus lookupRoleStatus(int key) {
    return appState.lookupRoleStatus(key);
  }

  @Override
  public RoleStatus lookupRoleStatus(Container c) throws YarnRuntimeException {
    return appState.lookupRoleStatus(c);
  }

  @Override
  public RoleStatus lookupRoleStatus(String name) throws YarnRuntimeException {
    return appState.lookupRoleStatus(name);
  }

  @Override
  public List<RoleInstance> cloneOwnedContainerList() {
    return appState.cloneOwnedContainerList();
  }

  @Override
  public int getNumOwnedContainers() {
    return appState.getNumOwnedContainers();
  }

  @Override
  public RoleInstance getOwnedContainer(ContainerId id) {
    return appState.getOwnedContainer(id);
  }

  @Override
  public RoleInstance getOwnedContainer(String id) throws NoSuchNodeException {
    return appState.getOwnedInstanceByContainerID(id);
  }

  @Override
  public List<RoleInstance> cloneLiveContainerInfoList() {
    return appState.cloneLiveContainerInfoList();
  }

  @Override
  public RoleInstance getLiveInstanceByContainerID(String containerId) throws
      NoSuchNodeException {
    return appState.getLiveInstanceByContainerID(containerId);
  }

  @Override
  public List<RoleInstance> getLiveInstancesByContainerIDs(Collection<String> containerIDs) {
    return appState.getLiveInstancesByContainerIDs(containerIDs);
  }

  @Override
  public void refreshClusterStatus() {
    appState.refreshClusterStatus();
  }

}
