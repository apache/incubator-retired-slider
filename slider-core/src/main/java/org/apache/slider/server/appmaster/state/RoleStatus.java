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
import org.apache.slider.server.appmaster.management.LongGauge;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Models the ongoing status of all nodes in an application.
 *
 * These structures are shared across the {@link AppState} and {@link RoleHistory} structures,
 * and must be designed for synchronous access. Atomic counters are preferred to anything which
 * requires synchronization. Where synchronized access is good is that it allows for
 * the whole instance to be locked, for updating multiple entries.
 */
public final class RoleStatus implements Cloneable {

  private final String name;

  /**
   * Role priority
   */
  private final int key;
  private final ProviderRole providerRole;

  private final LongGauge desired = new LongGauge();
  private final LongGauge actual = new LongGauge();
  private final LongGauge requested = new LongGauge();
  private final LongGauge releasing = new LongGauge();
  private final LongGauge failed = new LongGauge();
  private final LongGauge startFailed = new LongGauge();
  private final LongGauge started= new LongGauge();
  private final LongGauge completed = new LongGauge();
  private final LongGauge totalRequested = new LongGauge();
  private final LongGauge preempted = new LongGauge(0);
  private final LongGauge nodeFailed = new LongGauge(0);
  private final LongGauge failedRecently = new LongGauge(0);
  private final LongGauge limitsExceeded = new LongGauge(0);

  /** flag set to true if there is an outstanding anti-affine request */
  private final AtomicBoolean pendingAARequest = new AtomicBoolean(false);

  /**
   * Number of AA requests queued. These should be reduced first on a
   * flex down.
   */
  private int pendingAntiAffineRequestCount = 0;

  /** any pending AA request */
  public OutstandingRequest outstandingAArequest = null;

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

  public long getDesired() {
    return desired.get();
  }

  public void setDesired(long desired) {
    this.desired.set(desired);
  }

  public long getActual() {
    return actual.get();
  }

  public long incActual() {
    return actual.incrementAndGet();
  }

  public long decActual() {
    return actual.decToFloor(1);
  }

  public long getRequested() {
    return requested.get();
  }

  public long incRequested() {
    totalRequested.incrementAndGet();
    return requested.incrementAndGet();
  }

  
  public long cancel(long count) {
    return requested.decToFloor(count);
  }
  
  public void decRequested() {
    cancel(1);
  }

  public long getReleasing() {
    return releasing.get();
  }

  public long incReleasing() {
    return releasing.incrementAndGet();
  }

  public long decReleasing() {
    return releasing.decToFloor(1);
  }

  public long getFailed() {
    return failed.get();
  }

  public long getFailedRecently() {
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
        failed.incrementAndGet();
        break;

      case Failed_limits_exceeded: // exceeded memory or CPU; app/configuration related
        limitsExceeded.incrementAndGet();
        // fall through
      case Failed: // application failure, possibly node related, possibly not
      default: // anything else (future-proofing)
        failed.incrementAndGet();
        failedRecently.incrementAndGet();
        //have a look to see if it short lived
        if (startupFailure) {
          incStartFailed();
        }
        break;
    }
  }

  public long getStartFailed() {
    return startFailed.get();
  }

  public synchronized void incStartFailed() {
    startFailed.getAndIncrement();
  }

  public synchronized String getFailureMessage() {
    return failureMessage;
  }

  public long getCompleted() {
    return completed.get();
  }

  public synchronized void setCompleted(int completed) {
    this.completed.set(completed);
  }

  public long incCompleted() {
    return completed.incrementAndGet();
  }
  public long getStarted() {
    return started.get();
  }

  public synchronized void incStarted() {
    started.incrementAndGet();
  }

  public long getTotalRequested() {
    return totalRequested.get();
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
  public long getDelta() {
    long inuse = getActualAndRequested();
    //don't know how to view these. Are they in-use or not?
    long delta = desired.get() - inuse;
    if (delta < 0) {
      //if we are releasing, remove the number that are already released.
      delta += releasing.get();
      //but never switch to a positive
      delta = Math.min(delta, 0);
    }
    return delta;
  }

  /**
   * Get count of actual and requested containers
   * @return the size of the application when outstanding requests are included
   */
  public long getActualAndRequested() {
    return actual.get() + requested.get();
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
           ", pendingAntiAffineRequestCount=" + pendingAntiAffineRequestCount +
           ", failed=" + failed +
           ", failed recently=" + failedRecently.get() +
           ", node failed=" + nodeFailed.get() +
           ", pre-empted=" + preempted.get() +
           ", started=" + started +
           ", startFailed=" + startFailed +
           ", completed=" + completed +
           ", failureMessage='" + failureMessage + '\'' +
           ", providerRole=" + providerRole +
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
    info.desired = desired.intValue();
    info.actual = actual.intValue();
    info.requested = requested.intValue();
    info.releasing = releasing.intValue();
    info.failed = failed.intValue();
    info.startFailed = startFailed.intValue();
    info.placementPolicy = getPlacementPolicy();
    info.failureMessage = failureMessage;
    info.totalRequested = totalRequested.intValue();
    info.failedRecently = failedRecently.intValue();
    info.nodeFailed = nodeFailed.intValue();
    info.preempted = preempted.intValue();
    info.pendingAntiAffineRequest = pendingAARequest.get();
    info.pendingAntiAffineRequestCount = pendingAntiAffineRequestCount;
    return info;
  }

  /**
   * Get the (possibly null) label expression for this role
   * @return a string or null
   */
  public String getLabelExpression() {
    return providerRole.labelExpression;
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
