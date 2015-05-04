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

import org.apache.slider.api.types.ComponentInformation;
import org.apache.slider.providers.PlacementPolicy;
import org.apache.slider.providers.ProviderRole;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Models the ongoing status of all nodes in  
 * Nothing here is synchronized: grab the whole instance to update.
 */
public final class RoleStatus implements Cloneable {

  private final String name;

  /**
   * Role priority
   */
  private final int key;

  private final ProviderRole providerRole;

  private int desired, actual, requested, releasing;
  private int failed, startFailed;
  private int started,  completed, totalRequested;
  private final AtomicLong preempted = new AtomicLong(0);
  private final AtomicLong nodeFailed = new AtomicLong(0);
  private final AtomicLong failedRecently = new AtomicLong(0);
  private final AtomicLong limitsExceeded = new AtomicLong(0);

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

  public long getPlacementTimeoutSeconds() {
    return providerRole.placementTimeoutSeconds;
  }
  
  /**
   * The number of failures on a specific node that can be tolerated
   * before selecting a different node for placement
   * @return
   */
  public int getNodeFailureThreshold() {
    return providerRole.nodeFailureThreshold;
  }

  public boolean isExcludeFromFlexing() {
    return hasPlacementPolicy(PlacementPolicy.EXCLUDE_FROM_FLEXING);
  }

  public boolean isStrictPlacement() {
    return hasPlacementPolicy(PlacementPolicy.STRICT);
  }

  public boolean isAntiAffinePlacement() {
    return hasPlacementPolicy(PlacementPolicy.ANTI_AFFINITY_REQUIRED);
  }

  public boolean hasPlacementPolicy(int policy) {
    return 0 != (getPlacementPolicy() & policy);
  }

  public boolean isPlacementDesired() {
    return !hasPlacementPolicy(PlacementPolicy.NO_DATA_LOCALITY);
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

  public synchronized long getFailedRecently() {
    return failedRecently.get();
  }

  /**
   * Reset the recent failure
   * @return the number of failures in the "recent" window
   */
  public long resetFailedRecently() {
    return failedRecently.getAndSet(0);
  }

  public long getLimitsExceeded() {
    return limitsExceeded.get();
  }

  /**
   * Note that a role failed, text will
   * be used in any diagnostics if an exception
   * is later raised.
   * @param startupFailure flag to indicate this was a startup event
   * @param text text about the failure
   * @param outcome outcome of the container
   */
  public synchronized void noteFailed(boolean startupFailure, String text,
      ContainerOutcome outcome) {
    if (text != null) {
      failureMessage = text;
    }
    switch (outcome) {
      case Preempted:
        preempted.incrementAndGet();
        break;

      case Node_failure:
        nodeFailed.incrementAndGet();
        failed++;
        break;

      case Failed_limits_exceeded: // exceeded memory or CPU; app/configuration related
        limitsExceeded.incrementAndGet();
        // fall through
      case Failed: // application failure, possibly node related, possibly not
      default: // anything else (future-proofing)
        failed++;
        failedRecently.incrementAndGet();
        //have a look to see if it short lived
        if (startupFailure) {
          incStartFailed();
        }
        break;
    }
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

  public long getPreempted() {
    return preempted.get();
  }

  public long getNodeFailed() {
    return nodeFailed.get();
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
           ", desired=" + desired +
           ", actual=" + actual +
           ", requested=" + requested +
           ", releasing=" + releasing +
           ", failed=" + failed +
           ", failed recently=" + failedRecently.get() +
           ", node failed=" + nodeFailed.get() +
           ", pre-empted=" + preempted.get() +
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
  public Map<String, Integer> buildStatistics() {
    ComponentInformation componentInformation = serialize();
    return componentInformation.buildStatistics();
  }

  /**
   * Produced a serialized form which can be served up as JSON
   * @return a summary of the current role status.
   */
  public synchronized ComponentInformation serialize() {
    ComponentInformation info = new ComponentInformation();
    info.name = name;
    info.priority = getPriority();
    info.desired = desired;
    info.actual = actual;
    info.requested = requested;
    info.releasing = releasing;
    info.failed = failed;
    info.startFailed = startFailed;
    info.placementPolicy = getPlacementPolicy();
    info.failureMessage = failureMessage;
    info.totalRequested = totalRequested;
    info.failedRecently = failedRecently.intValue();
    info.nodeFailed = nodeFailed.intValue();
    info.preempted = preempted.intValue();
    return info;
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
