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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service that calls the supplied callback when it is started -after the 
 * given delay, then stops itself.
 * The notifications come in on a callback thread -a thread that is only
 * started in this service's <code>start()</code> operation.
 */
public class WorkflowEventNotifyingService extends WorkflowExecutorService
    implements Runnable {
  protected static final Logger LOG =
    LoggerFactory.getLogger(WorkflowEventNotifyingService.class);
  private final WorkflowEventCallback callback;
  private final int delay;
  private final ServiceTerminatingRunnable command;
  private final Object parameter;


  /**
   * Create an instance of the service
   * @param name service name
   * @param callback callback to invoke
   * @param parameter optional parameter for the callback
   * @param delay delay -or 0 for no delay
   */
  public WorkflowEventNotifyingService(String name,
      WorkflowEventCallback callback,
      Object parameter,
      int delay) {
    super(name);
    Preconditions.checkNotNull(callback, "Null callback argument");
    this.callback = callback;
    this.delay = delay;
    this.parameter = parameter;
    command = new ServiceTerminatingRunnable(this, this);
  }

  /**
   * Create an instance of the service
   * @param callback callback to invoke
   * @param parameter optional parameter for the callback
   * @param delay delay -or 0 for no delay
   */
  public WorkflowEventNotifyingService(WorkflowEventCallback callback,
      Object parameter,
      int delay) {
    this("WorkflowEventNotifyingService", callback, parameter, delay);
  }

  @Override
  protected void serviceStart() throws Exception {
    LOG.debug("Notifying {} after a delay of {} millis", callback, delay);
    setExecutor(ServiceThreadFactory.newSingleThreadExecutor(getName(), true));
    execute(command);
  }

  /**
   * Stop the service.
   * If there is any exception noted from any executed notification,
   * note the exception in this class
   * @throws Exception exception.
   */
  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    // propagate any failure
    if (command != null && command.getException() != null) {
      noteFailure(command.getException());
    }
  }

  /**
   * Perform the work in a different thread. Relies on
   * the {@link ServiceTerminatingRunnable} to trigger
   * the service halt on this thread.
   */
  @Override // Runnable
  public void run() {
    if (delay > 0) {
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted: {} in runnable", e, e);
      }
    }
    LOG.debug("Notifying {}", callback);
    callback.eventCallbackEvent(parameter);
  }

}
