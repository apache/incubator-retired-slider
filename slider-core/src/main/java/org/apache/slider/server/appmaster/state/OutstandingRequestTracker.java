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

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.server.appmaster.operations.AbstractRMOperation;
import org.apache.slider.server.appmaster.operations.CancelSingleRequest;
import org.apache.slider.server.appmaster.operations.ContainerRequestOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Tracks outstanding requests made with a specific placement option.
 * <p>
 *   <ol>
 *     <li>Used to decide when to return a node to 'can request containers here' list</li>
 *     <li>Used to identify requests where placement has timed out, and so issue relaxed requests</li>
 *   </ol>
 * <p>
 * If an allocation comes in that is not in the map: either the allocation
 * was unplaced, or the placed allocation could not be met on the specified
 * host, and the RM/scheduler fell back to another location. 
 */

public class OutstandingRequestTracker {
  protected static final Logger log =
    LoggerFactory.getLogger(OutstandingRequestTracker.class);

  /**
   * no requests; saves creating a new list if not needed
   */
  private final List<AbstractRMOperation> NO_REQUESTS = new ArrayList<>(0);
 
  private Map<OutstandingRequest, OutstandingRequest> placedRequests =
    new HashMap<>();

  /**
   * Create a new request for the specific role. If a
   * location is set, the request is added to the list of requests to track.
   * if it isn't, it is not tracked.
   * <p>
   * This does not update the node instance's role's request count
   * @param instance node instance to manager
   * @param role role index
   * @return a new request
   */
  public synchronized OutstandingRequest newRequest(NodeInstance instance, int role) {
    OutstandingRequest request =
      new OutstandingRequest(role, instance);
    if (request.isLocated()) {
      placedRequests.put(request, request);
    }
    return request;
  }

  /**
   * Look up any oustanding request to a (role, hostname). 
   * @param role role index
   * @param hostname hostname
   * @return the request or null if there was no outstanding one in the {@link #placedRequests}
   */
  public synchronized OutstandingRequest lookup(int role, String hostname) {
    return placedRequests.get(new OutstandingRequest(role, hostname));
  }

  /**
   * Remove a request
   * @param request matching request to find
   * @return the request or null for no match in the {@link #placedRequests}
   */
  public synchronized OutstandingRequest remove(OutstandingRequest request) {
    return placedRequests.remove(request);
  }

  /**
   * Notification that a container has been allocated -drop it
   * from the {@link #placedRequests} structure.
   * @param role role index
   * @param hostname hostname
   * @return true if an entry was found and removed
   */
  public synchronized boolean onContainerAllocated(int role, String hostname) {
    OutstandingRequest request =
      placedRequests.remove(new OutstandingRequest(role, hostname));
    if (request == null) {
      return false;
    } else {
      //satisfied request
      request.completed();
    }
    return true;
  }
  
  /**
   * Determine which host was a role type most recently used on, so that
   * if a choice is made of which (potentially surplus) containers to use,
   * the most recent one is picked first. This operation <i>does not</i>
   * change the role history, though it queries it.
   */
  static class newerThan implements Comparator<Container>, Serializable {
    private RoleHistory rh;
    
    public newerThan(RoleHistory rh) {
      this.rh = rh;
    }

    /**
     * Get the age of a node hosting container. If it is not known in the history, 
     * return 0.
     * @param c container
     * @return age, null if there's no entry for it. 
     */
    private long getAgeOf(Container c) {
      long age = 0;
      NodeInstance node = rh.getExistingNodeInstance(c);
      int role = ContainerPriority.extractRole(c);
      if (node != null) {
        NodeEntry nodeEntry = node.get(role);
        if (nodeEntry != null) {
          age = nodeEntry.getLastUsed();
        }
      }
      return age;
    }

    /**
     * Comparator: which host is more recent?
     * @param c1 container 1
     * @param c2 container 2
     * @return 1 if c2 older-than c1, 0 if equal; -1 if c1 older-than c2
     */
    @Override
    public int compare(Container c1, Container c2) {
      int role1 = ContainerPriority.extractRole(c1);
      int role2 = ContainerPriority.extractRole(c2);
      if (role1 < role2) return -1;
      if (role1 > role2) return 1;

      long age = getAgeOf(c1);
      long age2 = getAgeOf(c2);

      if (age > age2) {
        return -1;
      } else if (age < age2) {
        return 1;
      }
      // equal
      return 0;
    }
  }

  /**
   * Take a list of requests and split them into specific host requests and
   * generic assignments. This is to give requested hosts priority
   * in container assignments if more come back than expected
   * @param rh RoleHistory instance
   * @param inAllocated the list of allocated containers
   * @param outPlaceRequested initially empty list of requested locations 
   * @param outUnplaced initially empty list of unrequested hosts
   */
  public synchronized void partitionRequests(RoleHistory rh,
      List<Container> inAllocated,
      List<Container> outPlaceRequested,
      List<Container> outUnplaced) {
    Collections.sort(inAllocated, new newerThan(rh));
    for (Container container : inAllocated) {
      int role = ContainerPriority.extractRole(container);
      String hostname = RoleHistoryUtils.hostnameOf(container);
      if (placedRequests.containsKey(new OutstandingRequest(role, hostname))) {
        outPlaceRequested.add(container);
      } else {
        outUnplaced.add(container);
      }
    }
  }
  

  /**
   * Reset list all outstanding requests for a role: return the hostnames
   * of any canceled requests
   *
   * @param role role to cancel
   * @return possibly empty list of hostnames
   */
  public synchronized List<NodeInstance> resetOutstandingRequests(int role) {
    List<NodeInstance> hosts = new ArrayList<>();
    Iterator<Map.Entry<OutstandingRequest,OutstandingRequest>> iterator =
      placedRequests.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<OutstandingRequest, OutstandingRequest> next =
        iterator.next();
      OutstandingRequest request = next.getKey();
      if (request.roleId == role) {
        iterator.remove();
        request.completed();
        hosts.add(request.node);
      }
    }
    return hosts;
  }

  /**
   * Get a list of outstanding requests. The list is cloned, but the contents
   * are shared
   * @return a list of the current outstanding requests
   */
  public synchronized List<OutstandingRequest> listOutstandingRequests() {
    return new ArrayList<>(placedRequests.values());
  }

  /**
   * Escalate operation as triggered by external timer.
   * @return a (usually empty) list of cancel/request operations.
   */
  public synchronized List<AbstractRMOperation> escalateOutstandingRequests(long now) {
    if (placedRequests.isEmpty()) {
      return NO_REQUESTS;
    }

    List<AbstractRMOperation> operations = new ArrayList<>();
    for (OutstandingRequest outstandingRequest : placedRequests.values()) {
      synchronized (outstandingRequest) {
        // sync escalation check with operation so that nothing can happen to state
        // of the request during the escalation
        if (outstandingRequest.shouldEscalate(now)) {

          // time to escalate
          CancelSingleRequest cancel = new CancelSingleRequest(outstandingRequest.issuedRequest);
          operations.add(cancel);
          AMRMClient.ContainerRequest escalated =
              outstandingRequest.escalate();
          operations.add(new ContainerRequestOperation(escalated));
        }
      }
      
    }
    return operations;
  }
}
