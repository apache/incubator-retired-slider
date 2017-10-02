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

package org.apache.slider.server.appmaster.actions;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.slider.api.SliderExitReason;
import org.apache.slider.core.main.LauncherExitCodes;
import org.apache.slider.server.appmaster.SliderAppMaster;
import org.apache.slider.server.appmaster.state.AppState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors at a regular interval if the container health for a specific role
 * has dropped below a desired threshold.
 */
public class MonitorHealthThreshold extends AsyncAction {
  protected static final Logger log = LoggerFactory
      .getLogger(MonitorHealthThreshold.class);

  private final String roleGroup;
  private final int healthThresholdPercent;
  private final long healthThresholdWindowSecs;
  private final long healthThresholdWindowNanos;
  private long firstOccurrenceTimestamp = 0;
  // Sufficient logging happens when role health is below threshold. However,
  // there has to be some logging when it is above threshold, otherwise app
  // owners have no idea how the health is fluctuating. So let's log whenever
  // there is a change in role health, thereby preventing excessive logging on
  // every poll. 
  private float prevRunningContainerFraction = 0;

  public MonitorHealthThreshold(String roleGroup, int healthThresholdPercent,
      long healthThresholdWindowSecs) {
    super("MonitorHealthThreshold");
    this.roleGroup = roleGroup;
    this.healthThresholdPercent = healthThresholdPercent;
    this.healthThresholdWindowSecs = healthThresholdWindowSecs;
    this.healthThresholdWindowNanos = TimeUnit.NANOSECONDS
        .convert(healthThresholdWindowSecs, TimeUnit.SECONDS);
  }

  @Override
  public void execute(SliderAppMaster appMaster, QueueAccess queueService,
      AppState appState) throws Exception {
    log.debug("MonitorHealthThreshold execute method");
    // Perform container health checks against desired threshold
    synchronized (appMaster) {
      long desiredContainerCount = appState.getDesiredContainerCount(roleGroup);
      // if desired container count for this role is 0 then nothing to do
      if (desiredContainerCount == 0) {
        return;
      }
      long runningContainerCount = appState.getLiveContainerCount(roleGroup);
      float thresholdFraction = (float) healthThresholdPercent / 100;
      // no possibility of div by 0 since desiredContainerCount won't be 0 here
      float runningContainerFraction = (float) runningContainerCount
          / desiredContainerCount;
      boolean healthChanged = false;
      if (runningContainerFraction != prevRunningContainerFraction) {
        prevRunningContainerFraction = runningContainerFraction;
        healthChanged = true;
      }
      String runningContainerPercentStr = String.format("%.2f",
          runningContainerFraction * 100);
      // Check if the current running container percent is less than the
      // threshold percent
      if (runningContainerFraction < thresholdFraction) {
        // Check if it is the first occurrence and if yes set the timestamp
        long currentTimestamp = now();
        if (firstOccurrenceTimestamp == 0) {
          firstOccurrenceTimestamp = currentTimestamp;
          log.info("Role {} is going below health threshold for the first time "
              + "at ts = {}", roleGroup, firstOccurrenceTimestamp);
        }
        long elapsedTime = currentTimestamp - firstOccurrenceTimestamp;
        long elapsedTimeSecs = TimeUnit.SECONDS.convert(elapsedTime,
            TimeUnit.NANOSECONDS);
        log.warn(
            "Role = {}, Current health = {}%, is below Health threshold of {}% "
                + "for {} secs (window = {} secs)",
            roleGroup, runningContainerPercentStr, healthThresholdPercent,
            elapsedTimeSecs, healthThresholdWindowSecs);
        if (elapsedTime > healthThresholdWindowNanos) {
          log.error(
              "Role = {}, Current health = {}%, has been below health "
                  + "threshold of {}% for {} secs (threshold window = {} secs)",
              roleGroup, runningContainerPercentStr, healthThresholdPercent,
              elapsedTimeSecs, healthThresholdWindowSecs);
          // Trigger an app stop
          ActionStopSlider stopSlider = new ActionStopSlider("stop",
              LauncherExitCodes.EXIT_EXCEPTION_THROWN,
              FinalApplicationStatus.FAILED,
              String.format(
                  "Application was killed because container health for role %s "
                      + "was %s%% (threshold = %d%%) for %d secs (threshold "
                      + "window = %d secs)",
                  roleGroup, runningContainerPercentStr, healthThresholdPercent,
                  elapsedTimeSecs, healthThresholdWindowSecs));
          stopSlider.setExitReason(SliderExitReason.APP_ERROR);
          appMaster.queue(stopSlider);
        }
      } else {
        String logMsg = "Role = {}, Health threshold = {}%, Current health = "
            + "{}% (Current Running count = {}, Desired count = {})";
        if (healthChanged) {
          log.info(logMsg, roleGroup, healthThresholdPercent,
              runningContainerPercentStr, runningContainerCount,
              desiredContainerCount);
        } else {
          log.debug(logMsg, roleGroup, healthThresholdPercent,
              runningContainerPercentStr, runningContainerCount,
              desiredContainerCount);
        }
        // The container health might have recovered above threshold after being
        // below for less than the threshold window amount of time. So we need
        // to reset firstOccurrenceTimestamp to 0.
        if (firstOccurrenceTimestamp != 0) {
          log.info(
              "Role = {}, resetting first occurence to 0, since it recovered "
                  + "above health threshold of {}%",
              roleGroup, healthThresholdPercent);
          firstOccurrenceTimestamp = 0;
        }
      }
    }
  }
}
