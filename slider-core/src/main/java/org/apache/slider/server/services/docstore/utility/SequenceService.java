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

package org.apache.slider.server.services.docstore.utility;

import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.service.ServiceStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This resembles the YARN CompositeService, except that it
 * starts one service after another: it's init & start operations
 * only work with one service
 */

public class SequenceService extends AbstractService implements Parent,
                                                     ServiceStateChangeListener {

  private static final Logger log =
    LoggerFactory.getLogger(SequenceService.class);

  /**
   * list of services
   */
  private final List<Service> serviceList = new ArrayList<Service>();

  /**
   * The current service.
   * Volatile -may change & so should be read into a 
   * local variable before working with
   */
  private volatile Service currentService;
  /*
  the previous service -the last one that finished. 
  Null if one did not finish yet
   */
  private volatile Service previousService;

  /**
   * Create a service sequence with the given list of services
   * @param name service name
   * @param offspring initial sequence
   */
   public SequenceService(String name, Service...offspring) {
    super(name);
     for (Service service : offspring) {
       addService(service);
     }
  }

  /**
   * Get the current service -which may be null
   * @return service running
   */
  public Service getCurrentService() {
    return currentService;
  }

  public Service getPreviousService() {
    return previousService;
  }

  /**
   * When started
   * @throws Exception
   */
  @Override
  protected void serviceStart() throws Exception {
    startNextService();
  }

  @Override
  protected void serviceStop() throws Exception {
    //stop current service.
    //this triggers a callback that is caught and ignored
    Service current = currentService;
    previousService = current;
    currentService = null;
    if (current != null) {
      current.stop();
    }
  }


  /**
   * Start the next service in the list.
   * Return false if there are no more services to run, or this
   * service has stopped
   * @return true if a service was started
   * @throws RuntimeException from any init or start failure
   * @throws ServiceStateException if this call is made before
   * the service is started
   */
  public synchronized boolean startNextService() {
    if (isInState(STATE.STOPPED)) {
      //downgrade to a failed
      log.debug("Not starting next service -{} is stopped", this);
      return false;
    }
    if (!isInState(STATE.STARTED)) {
      //reject attempts to start a service too early
      throw new ServiceStateException(
        "Cannot start a child service when not started");
    }
    if (serviceList.isEmpty()) {
      //nothing left to run
      return false;
    }
    if (currentService != null && currentService.getFailureCause() != null) {
      //did the last service fail? Is this caused by some premature callback?
      log.debug("Not starting next service due to a failure of {}",
                currentService);
      return false;
    }
    //bear in mind that init & start can fail, which
    //can trigger re-entrant calls into the state change listener.
    //by setting the current service to null
    //the start-next-service logic is skipped.
    //now, what does that mean w.r.t exit states?

    currentService = null;
    Service head = serviceList.remove(0);

    try {
      head.init(getConfig());
      head.registerServiceListener(this);
      head.start();
    } catch (RuntimeException e) {
      noteFailure(e);
      throw e;
    }
    //at this point the service must have explicitly started & not failed,
    //else an exception would have been raised
    currentService = head;
    return true;
  }

  /**
   * State change event relays service stop events to
   * {@link #onServiceCompleted(Service)}. Subclasses can
   * extend that with extra logic
   * @param service the service that has changed.
   */
  @Override
  public void stateChanged(Service service) {
    if (service == currentService && service.isInState(STATE.STOPPED)) {
      onServiceCompleted(service);
    }
  }

  /**
   * handler for service completion: base class starts the next service
   * @param service service that has completed
   */
  protected synchronized void onServiceCompleted(Service service) {
    log.info("Running service stopped: {}", service);
    previousService = currentService;
    

    //start the next service if we are not stopped ourselves
    if (isInState(STATE.STARTED)) {

      //did the service fail? if so: propagate
      Throwable failureCause = service.getFailureCause();
      if (failureCause != null) {
        Exception e = SliderServiceUtils.convertToException(failureCause);
        noteFailure(e);
        stop();
      }
      
      //start the next service
      boolean started;
      try {
        started = startNextService();
      } catch (Exception e) {
        //something went wrong here
        noteFailure(e);
        started = false;
      }
      if (!started) {
        //no start because list is empty
        //stop and expect the notification to go upstream
        stop();
      }
    } else {
      //not started, so just note that the current service
      //has gone away
      currentService = null;
    }
  }

  /**
   * Add the passed {@link Service} to the list of services managed by this
   * {@link SequenceService}
   * @param service the {@link Service} to be added
   */
  @Override //Parent
  public synchronized void addService(Service service) {
    log.debug("Adding service {} ", service.getName());
    synchronized (serviceList) {
      serviceList.add(service);
    }
  }

  /**
   * Get an unmodifiable list of services
   * @return a list of child services at the time of invocation -
   * added services will not be picked up.
   */
  @Override //Parent
  public synchronized List<Service> getServices() {
    return Collections.unmodifiableList(serviceList);
  }

  @Override // Object
  public synchronized String toString() {
    return super.toString() + "; current service " + currentService
           + "; queued service count=" + serviceList.size();
  }

}
