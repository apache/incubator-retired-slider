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

import org.apache.slider.server.appmaster.SliderAppMaster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This action executes then reschedules an inner action; a limit
 * can specify the number of times to run
 */

public class RenewingAction extends AsyncAction{
  private static final Logger log =
      LoggerFactory.getLogger(RenewingAction.class);
  private final AsyncAction action;
  private final int interval;
  private final TimeUnit timeUnit;
  public final AtomicInteger executionCount = new AtomicInteger();
  public final int limit;


  /**
   * Rescheduling action
   * @param action action to execute
   * @param initialDelay initial delay
   * @param interval interval for later delays
   * @param timeUnit time unit for all times
   * @param limit limit on the no. of executions. If 0 or less: no limit
   */
  public RenewingAction(AsyncAction action,
      int initialDelay,
      int interval, TimeUnit timeUnit,
      int limit) {
    super("renewing " + action.name, initialDelay, timeUnit, action.getAttrs());
    this.action = action;
    this.interval = interval;
    this.timeUnit = timeUnit;
    this.limit = limit;
  }

  /**
   * Execute the inner action then reschedule ourselves
   * @param appMaster
   * @param queueService
   * @throws Exception
   */
  @Override
  public void execute(SliderAppMaster appMaster, QueueAccess queueService)
      throws Exception {
    long exCount = executionCount.incrementAndGet();
    log.debug("{}: Executing inner action count # {}", this, exCount);
    action.execute(appMaster, queueService);
    boolean reschedule = true;
    if (limit > 0) {
      reschedule = limit > exCount;
    }
    if (reschedule) {
      this.setNanos(convertAndOffset(interval, timeUnit));
      log.debug("{}: rescheduling, new offset {} mS ", this,
          getDelay(TimeUnit.MILLISECONDS));
      queueService.putDelayed(this);
    }
  }

  public AsyncAction getAction() {
    return action;
  }

  public int getInterval() {
    return interval;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public int getExecutionCount() {
    return executionCount.get();
  }

  public int getLimit() {
    return limit;
  }
}
