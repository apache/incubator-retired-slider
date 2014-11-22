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

import org.apache.slider.api.StatusKeys;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;


/**
 * Models the ongoing status of all nodes in  
 * Nothing here is synchronized: grab the whole instance to update.
 */
public final class RoleStatus implements Cloneable {

  private final String name;

  /**
   * Role key in the container details stored in the AM,
   * currently mapped to priority
   */
  private final int key;

  private final ProviderRole providerRole;

  private int desired, actual, requested, releasing;
  private int failed, started, startFailed, completed, totalRequested;

  /**
   * value to use when specifiying "no limit" for instances: {@value}
   */
  public static final int UNLIMITED_INSTANCES = 1;

  /**
   * minimum number of instances of a role permitted in a valid
   * configuration. Default: 0.
   */
  private int minimum = 0;

  /**
   * maximum number of instances of a role permitted in a valid
   * configuration. Default: unlimited.
   */
  private int maximum = UNLIMITED_INSTANCES;
  
  private String failureMessage = "";

  public RoleStatus(ProviderRole providerRole) {
    this.providerRole = providerRole;
    this.name = providerRole.name;
    this.key = providerRole.id;
  }
  
  public String getName() {
    return name;
  }

  public int getKey() {
    return key;
  }

  public int getPriority() {
    return getKey();
  }

  /**
   * Get the placement policy enum, from the values in
   * {@link PlacementPolicy}
   * @return the placement policy for this role
   */
  public int getPlacementPolicy() {
    return providerRole.placementPolicy;
  }

  public boolean getExcludeFromFlexing() {
    return 0 != (getPlacementPolicy() & PlacementPolicy.EXCLUDE_FROM_FLEXING);
  }
  
  public boolean getNoDataLocality() {
    return 0 != (getPlacementPolicy() & PlacementPolicy.NO_DATA_LOCALITY);
  }
  
  public boolean isStrictPlacement() {
    return 0 != (getPlacementPolicy() & PlacementPolicy.STRICT);
  }

  public synchronized int getDesired() {
    return desired;
  }

  public synchronized void setDesired(int desired) {
    this.desired = desired;
  }

  public synchronized int getActual() {
    return actual;
  }

  public synchronized int incActual() {
    return ++actual;
  }

  public synchronized int decActual() {
    actual = Math.max(0, actual - 1);
    return actual;
  }

  public synchronized int getRequested() {
    return requested;
  }

  public synchronized int incRequested() {
    totalRequested++;
    return ++requested;
  }

  public synchronized int cancel(int count) {
    requested = Math.max(0, requested - count);
    return requested;
  }
  
  public synchronized int decRequested() {
    return cancel(1);
  }

  public synchronized int getReleasing() {
    return releasing;
  }

  public synchronized int incReleasing() {
    return ++releasing;
  }

  public synchronized int decReleasing() {
    releasing = Math.max(0, releasing - 1);
    return releasing;
  }

  public synchronized int getFailed() {
    return failed;
  }

  /**
   * Reset the failure counts
   * @return the total number of failures up to this point
   */
  public synchronized int resetFailed() {
    int total = failed + startFailed;
    failed = 0;
    startFailed = 0;
    return total;
  }

  /**
   * Note that a role failed, text will
   * be used in any diagnostics if an exception
   * is later raised.
   * @param startupFailure flag to indicate this was a startup event
   * @return the number of failures
   * @param text text about the failure
   */
  public synchronized int noteFailed(boolean startupFailure, String text) {
    int current = ++failed;
    if (text != null) {
      failureMessage = text;
    }
    //have a look to see if it short lived
    if (startupFailure) {
      incStartFailed();
    }
    return current;
  }

  public synchronized int getStartFailed() {
    return startFailed;
  }

  public synchronized void incStartFailed() {
    startFailed++;
  }

  public synchronized String getFailureMessage() {
    return failureMessage;
  }

  public synchronized int getCompleted() {
    return completed;
  }

  public synchronized void setCompleted(int completed) {
    this.completed = completed;
  }

  public synchronized int incCompleted() {
    return completed ++;
  }
  public synchronized int getStarted() {
    return started;
  }

  public synchronized void incStarted() {
    started++;
  }

  public synchronized int getTotalRequested() {
    return totalRequested;
  }

  
  /**
   * Get the number of roles we are short of.
   * nodes released are ignored.
   * @return the positive or negative number of roles to add/release.
   * 0 means "do nothing".
   */
  public synchronized int getDelta() {
    int inuse = getActualAndRequested();
    //don't know how to view these. Are they in-use or not?
    int delta = desired - inuse;
    if (delta < 0) {
      //if we are releasing, remove the number that are already released.
      delta += releasing;
      //but never switch to a positive
      delta = Math.min(delta, 0);
    }
    return delta;
  }

  /**
   * Get count of actual and requested containers
   * @return the size of the application when outstanding requests are included
   */
  public synchronized int getActualAndRequested() {
    return actual + requested;
  }

  @Override
  public synchronized String toString() {
    return "RoleStatus{" +
           "name='" + name + '\'' +
           ", key=" + key +
           ", minimum=" + minimum +
           ", maximum=" + maximum +
           ", desired=" + desired +
           ", actual=" + actual +
           ", requested=" + requested +
           ", releasing=" + releasing +
           ", failed=" + failed +
           ", started=" + started +
           ", startFailed=" + startFailed +
           ", completed=" + completed +
           ", failureMessage='" + failureMessage + '\'' +
           '}';
  }

  @Override
  public synchronized  Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  /**
   * Get the provider role
   * @return the provider role
   */
  public ProviderRole getProviderRole() {
    return providerRole;
  }

  /**
   * Build the statistics map from the current data
   * @return a map for use in statistics reports
   */
  public synchronized Map<String, Integer> buildStatistics() {
    Map<String, Integer> stats = new HashMap<String, Integer>();
    stats.put(StatusKeys.STATISTICS_CONTAINERS_ACTIVE_REQUESTS, getRequested());
    stats.put(StatusKeys.STATISTICS_CONTAINERS_COMPLETED, getCompleted());
    stats.put(StatusKeys.STATISTICS_CONTAINERS_DESIRED, getDesired());
    stats.put(StatusKeys.STATISTICS_CONTAINERS_FAILED, getFailed());
    stats.put(StatusKeys.STATISTICS_CONTAINERS_LIVE, getActual());
    stats.put(StatusKeys.STATISTICS_CONTAINERS_REQUESTED, getTotalRequested());
    stats.put(StatusKeys.STATISTICS_CONTAINERS_STARTED, getStarted());
    stats.put(StatusKeys.STATISTICS_CONTAINERS_START_FAILED, getStartFailed());
    return stats;
  }

  /**
   * Compare two role status entries by name
   */
  public static class CompareByName implements Comparator<RoleStatus>,
      Serializable {
    @Override
    public int compare(RoleStatus o1, RoleStatus o2) {
      return o1.getName().compareTo(o2.getName());
    }
  }
  
  /**
   * Compare two role status entries by key
   */
  public static class CompareByKey implements Comparator<RoleStatus>,
      Serializable {
    @Override
    public int compare(RoleStatus o1, RoleStatus o2) {
      return (o1.getKey() < o2.getKey() ? -1 : (o1.getKey() == o2.getKey() ? 0 : 1));
    }
  }
  
}
