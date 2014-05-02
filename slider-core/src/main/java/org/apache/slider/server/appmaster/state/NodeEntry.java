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

/**
 * Information about the state of a role on a specific node instance.
 * No fields are synchronized; sync on the instance to work with it
 *
 The two fields `releasing` and `requested` are used to track the ongoing
 state of YARN requests; they do not need to be persisted across freeze/thaw
 cycles. They may be relevant across AM restart, but without other data
 structures in the AM, not enough to track what the AM was up to before
 it was restarted. The strategy will be to ignore unexpected allocation
 responses (which may come from pre-restart) requests, while treating
 unexpected container release responses as failures.

 The `active` counter is only decremented after a container release response
 has been received.
 
 Accesses are synchronized.
 */
public class NodeEntry {
  
  public final int index;

  public NodeEntry(int index) {
    this.index = index;
  }

  /**
   * instance explicitly requested on this node: it's OK if an allocation
   * comes in that has not been (and when that happens, this count should 
   * not drop)
   */
  private int requested;
  private int starting;
  private int startFailed;
  private int failed;
  /**
   * Number of live nodes. 
   */
  private int live;
  private int releasing;
  private long lastUsed;
  
  /**
   * Is the node available for assignments. This does not track
   * whether or not there are any outstanding requests for this node
   * @return true if there are no role instances here
   * other than some being released.
   */
  public synchronized boolean isAvailable() {
    return getActive() == 0 && (requested == 0) && starting == 0;
  }

  /**
   * return no of active instances -those that could be released as they
   * are live and not already being released
   * @return a number, possibly 0
   */
  public synchronized int getActive() {
    return (live - releasing);
  }

  /**
   * Return true if the node is not busy, and it
   * has not been used since the absolute time
   * @param absoluteTime time
   * @return true if the node could be cleaned up
   */
  public synchronized boolean notUsedSince(long absoluteTime) {
    return isAvailable() && lastUsed < absoluteTime;
  }

  public synchronized int getLive() {
    return live;
  }

  public int getStarting() {
    return starting;
  }

  /**
   * Set the live value directly -used on AM restart
   * @param v value
   */
  public synchronized void setLive(int v) {
    live = v;
  }
  
  private void incLive() {
    ++live;
  }

  private synchronized void decLive() {
    live = RoleHistoryUtils.decToFloor(live);
  }
  
  public synchronized void onStarting() {
    ++starting;
  }

  private void decStarting() {
    starting = RoleHistoryUtils.decToFloor(starting);
  }

  public synchronized void onStartCompleted() {
    decStarting();
    incLive();
  }
  
    /**
   * start failed -decrement the starting flag.
   * @return true if the node is now available
   */
  public synchronized boolean onStartFailed() {
    decStarting();
    ++startFailed;
    ++failed;
    return isAvailable();
  }
  
  /**
   * no of requests made of this role of this node. If it goes above
   * 1 there's a problem
   */
  public synchronized  int getRequested() {
    return requested;
  }

  /**
   * request a node: 
   */
  public synchronized void request() {
    ++requested;
  }

  /**
   * A request made explicitly to this node has completed
   */
  public synchronized void requestCompleted() {
    requested = RoleHistoryUtils.decToFloor(requested);
  }

  /**
   * No of instances in release state
   */
  public synchronized int getReleasing() {
    return releasing;
  }

  /**
   * Release an instance -which is no longer marked as active
   */
  public synchronized void release() {
    assert live > 0 : "no live nodes to release";
    releasing++;
  }

  /**
   * completion event, which can be a planned or unplanned
   * planned: dec our release count
   * unplanned: dec our live count
   * @param wasReleased true if this was planned
   * @return true if this node is now available
   */
  public synchronized boolean containerCompleted(boolean wasReleased) {
    if (wasReleased) {
      releasing = RoleHistoryUtils.decToFloor(releasing);
    } else {
      ++failed;
    }
    decLive();
    return isAvailable();
  }

  /**
   * Time last used.
   */
  public synchronized long getLastUsed() {
    return lastUsed;
  }

  public synchronized void setLastUsed(long lastUsed) {
    this.lastUsed = lastUsed;
  }

  public int getStartFailed() {
    return startFailed;
  }

  public synchronized int getFailed() {
    return failed;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("NodeEntry{");
    sb.append("requested=").append(requested);
    sb.append(", starting=").append(starting);
    sb.append(", live=").append(live);
    sb.append(", failed=").append(failed);
    sb.append(", startFailed=").append(startFailed);
    sb.append(", releasing=").append(releasing);
    sb.append(", lastUsed=").append(lastUsed);
    sb.append('}');
    return sb.toString();
  }
}
