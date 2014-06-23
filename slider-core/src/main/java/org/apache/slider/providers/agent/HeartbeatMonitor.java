/**
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
package org.apache.slider.providers.agent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/** Monitors the container state and heartbeats. */
public class HeartbeatMonitor implements Runnable {
  protected static final Logger log =
      LoggerFactory.getLogger(HeartbeatMonitor.class);
  private final int threadWakeupInterval; //1 minute
  private final AgentProviderService provider;
  private volatile boolean shouldRun = true;
  private Thread monitorThread = null;

  public HeartbeatMonitor(AgentProviderService provider, int threadWakeupInterval) {
    this.provider = provider;
    this.threadWakeupInterval = threadWakeupInterval;
  }

  public void shutdown() {
    shouldRun = false;
  }

  public void start() {
    log.info("Starting heartbeat monitor with interval {}", threadWakeupInterval);
    monitorThread = new Thread(this);
    monitorThread.start();
  }

  void join(long millis) throws InterruptedException {
    if (isAlive()) {
      monitorThread.join(millis);
    }
  }

  public boolean isAlive() {
    if (monitorThread != null) {
      return monitorThread.isAlive();
    }
    return false;
  }

  @Override
  public void run() {
    while (shouldRun) {
      try {
        log.debug("Putting monitor to sleep for " + threadWakeupInterval + " " +
                  "milliseconds");
        Thread.sleep(threadWakeupInterval);
        doWork();
      } catch (InterruptedException ex) {
        log.warn("Scheduler thread is interrupted going to stop", ex);
        shouldRun = false;
      } catch (Exception ex) {
        log.warn("Exception received", ex);
      } catch (Throwable t) {
        log.warn("ERROR", t);
      }
    }
  }

  /**
   * Every interval the current state of the container are checked. If the state is INIT or HEALTHY and no HB are
   * received in last check interval they are marked as UNHEALTHY. INIT is when the agent is started but it did not
   * communicate at all. HEALTHY being the AM has received heartbeats. After an interval as UNHEALTHY the container is
   * declared unavailable
   */
  private void doWork() {
    Map<String, ComponentInstanceState> componentStatuses = provider.getComponentStatuses();
    if (componentStatuses != null) {
      for (String containerLabel : componentStatuses.keySet()) {
        ComponentInstanceState componentInstanceState = componentStatuses.get(containerLabel);
        long timeSinceLastHeartbeat = System.currentTimeMillis() - componentInstanceState.getLastHeartbeat();

        if (timeSinceLastHeartbeat > threadWakeupInterval) {
          if (componentInstanceState.getContainerState() == ContainerState.HEALTHY ||
              componentInstanceState.getContainerState() == ContainerState.INIT) {
            componentInstanceState.setContainerState(ContainerState.UNHEALTHY);
            log.warn("Component {} marked UNHEALTHY. Last heartbeat received at {} approx. {} ms. back.",
                     containerLabel, componentInstanceState.getLastHeartbeat(),
                     timeSinceLastHeartbeat);
            continue;
          }
          if (componentInstanceState.getContainerState() == ContainerState.UNHEALTHY
              && timeSinceLastHeartbeat > threadWakeupInterval * 2) {
            componentInstanceState.setContainerState(ContainerState.HEARTBEAT_LOST);
            log.warn("Component {} marked HEARTBEAT_LOST. Last heartbeat received at {} approx. {} ms. back.",
                     containerLabel, componentInstanceState.getLastHeartbeat(),
                     timeSinceLastHeartbeat);
            this.provider.releaseContainer(containerLabel);
            continue;
          }
        }
      }
    }
  }
}
