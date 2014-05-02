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

package org.apache.slider.server.services.docstore.utility;

import org.apache.hadoop.service.AbstractService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service that calls the supplied callback when it is started -after the 
 * given delay, then stops itself.
 * Because it calls in on a different thread, it can be used for callbacks
 * that don't 
 */
public class EventNotifyingService extends AbstractService implements Runnable {
  protected static final Logger log =
    LoggerFactory.getLogger(EventNotifyingService.class);
  private final EventCallback callback;
  private final int delay;

  public EventNotifyingService(EventCallback callback, int delay) {
    super("EventNotifyingService");
    assert callback != null;
    this.callback = callback;
    this.delay = delay;
  }

  @Override
  protected void serviceStart() throws Exception {
    log.debug("Notifying {} after a delay of {} millis", callback, delay);
    new Thread(this, "event").start();
  }

  @Override
  public void run() {
    if (delay > 0) {
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {

      }
    }
    log.debug("Notifying {}", callback);
    callback.eventCallbackEvent();
    stop();
  }
}
