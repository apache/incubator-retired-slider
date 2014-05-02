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

import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * An extended composite service which not only makes the 
 * addService method public, it auto-registers
 * itself as a listener for state change events.
 * 
 * When all child services has stopped, this service stops itself.
 */
public class CompoundService extends CompositeService implements Parent,
                                                                 ServiceStateChangeListener {

  private static final Logger log =
    LoggerFactory.getLogger(CompoundService.class);

  public CompoundService(String name) {
    super(name);
  }


  public CompoundService() {
    super("CompoundService");
  }

  /**
   * Varargs constructor
   * @param children children
   */
  public CompoundService(Service ... children) {
    this();
    for (Service child : children) {
      addService(child);
    }
  }

  /**
   * Add a service, and register it
   * @param service the {@link Service} to be added.
   * Important: do not add a service to a parent during your own serviceInit/start,
   * in Hadoop 2.2; you will trigger a ConcurrentModificationException.
   */
  @Override
  public void addService(Service service) {
    service.registerServiceListener(this);
    super.addService(service);
  }

  /**
   * When this service is started, any service stopping with a failure
   * exception is converted immediately into a failure of this service, 
   * storing the failure and stopping ourselves.
   * @param child the service that has changed.
   */
  @Override
  public void stateChanged(Service child) {
    //if that child stopped while we are running:
    if (isInState(STATE.STARTED) && child.isInState(STATE.STOPPED)) {
      // a child service has stopped
      //did the child fail? if so: propagate
      Throwable failureCause = child.getFailureCause();
      if (failureCause != null) {
        log.info("Child service " + child + " failed", failureCause);
        //failure. Convert to an exception
        Exception e = SliderServiceUtils.convertToException(failureCause);
        //flip ourselves into the failed state
        noteFailure(e);
        stop();
      } else {
        log.info("Child service completed {}", child);
        if (areAllChildrenStopped()) {
          log.info("All children are halted: stopping");
          stop();
        }
      }
    }
  }

  private boolean areAllChildrenStopped() {
    List<Service> children = getServices();
    boolean stopped = true;
    for (Service child : children) {
      if (!child.isInState(STATE.STOPPED)) {
        stopped = false;
        break;
      }
    }
    return stopped;
  }
}
