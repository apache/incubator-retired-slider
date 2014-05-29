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

package org.apache.slider.server.services.workflow;

import com.google.common.base.Preconditions;
import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A service that calls the supplied callback when it is started -after the 
 * given delay, then stops itself.
 * The notifications come in on a different thread
 */
public class WorkflowEventNotifyingService extends AbstractService implements Runnable {
  protected static final Logger log =
    LoggerFactory.getLogger(WorkflowEventNotifyingService.class);
  private final WorkflowEventCallback callback;
  private final int delay;
  private ExecutorService executor;


  public WorkflowEventNotifyingService(String name,
      WorkflowEventCallback callback, int delay) {
    super(name);
    Preconditions.checkNotNull(callback, "Null callback argument");
    this.callback = callback;
    this.delay = delay;
  }

  public WorkflowEventNotifyingService(WorkflowEventCallback callback, int delay) {
    this("WorkflowEventNotifyingService", callback, delay);
  }

  @Override
  protected void serviceStart() throws Exception {
    log.debug("Notifying {} after a delay of {} millis", callback, delay);
    executor = Executors.newSingleThreadExecutor(
        new ServiceThreadFactory(getName(), true));
    executor.execute(this);
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  @Override // Runnable
  public void run() {
    if (delay > 0) {
      try {
        Thread.sleep(delay);
      } catch (InterruptedException interrupted) {
        return;
      }
    }
    log.debug("Notifying {}", callback);
    callback.eventCallbackEvent();
    stop();
  }
}
