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

import org.apache.hadoop.service.AbstractService;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * A service that hosts an executor -in shutdown it is stopped.
 */
public abstract class AbstractWorkflowExecutorService extends AbstractService {

  private ExecutorService executor;
  
  public AbstractWorkflowExecutorService(String name) {
    this(name, null);
  }

  protected AbstractWorkflowExecutorService(String name,
      ExecutorService executor) {
    super(name);
    this.executor = executor;
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  protected void setExecutor(ExecutorService executor) {
    this.executor = executor;
  }

  /**
   * Execute the runnable with the executor (which 
   * must have been created already)
   * @param runnable runnable to execute
   */
  public void execute(Runnable runnable) {
    executor.execute(runnable);
  }

  /**
   * Submit a callable
   * @param callable callable
   * @param <V> type of the final get
   * @return a future to wait on
   */
  public <V> Future<V> submit(Callable<V> callable) {
    return executor.submit(callable);
  }
  /**
   * Stop the service: halt the executor. 
   * @throws Exception exception.
   */
  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    stopExecutor();
  }

  /**
   * Stop the executor if it is not null.
   * This uses {@link ExecutorService#shutdownNow()}
   * and so does not block until they have completed.
   */
  protected void stopExecutor() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }
}
