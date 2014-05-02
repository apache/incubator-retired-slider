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
 * If an allocation comes in that is not in the map: either the allocation
 * was unplaced, or the placed allocation could not be met on the specified
 * host, and the RM/scheduler fell back to another location. 
 */

public class OutstandingRequestTracker {
  protected static final Logger log =
    LoggerFactory.getLogger(OutstandingRequestTracker.class);

  private Map<OutstandingRequest, OutstandingRequest> requests =
    new HashMap<OutstandingRequest, OutstandingRequest>();

  /**
   * Create a new request for the specific role. If a
   * location is set, the request is added to the list of requests to track.
   * if it isn't -it isn't.
   * This does not update the node instance's role's request count
   * @param instance node instance to manager
   * @param role role index
   * @return a new request
   */
  public synchronized OutstandingRequest addRequest(NodeInstance instance, int role) {
    OutstandingRequest request =
      new OutstandingRequest(role, instance);
    if (request.isLocated()) {
      requests.put(request, request);
    }
    return request;
  }

  /**
   * Look up any oustanding request to a (role, hostname). 
   * @param role role index
   * @param hostname hostname
   * @return the request or null if there was no outstanding one
   */
  public synchronized OutstandingRequest lookup(int role, String hostname) {
    return requests.get(new OutstandingRequest(role, hostname));
  }

  /**
   * Remove a request
   * @param request matching request to find
   * @return the request
   */
  public synchronized OutstandingRequest remove(OutstandingRequest request) {
    return requests.remove(request);
  }

  /**
   * Notification that a container has been allocated -drop it
   * from the list of outstanding roles if need be
   * @param role role index
   * @param hostname hostname
   * @return true if an entry was found and dropped
   */
  public synchronized boolean onContainerAllocated(int role, String hostname) {
    OutstandingRequest request =
      requests.remove(new OutstandingRequest(role, hostname));
    if (request == null) {
      return false;
    } else {
      //satisfied request
      request.completed();
    }
    return true;
  }

  static class newerThan implements Comparator<Container>, Serializable {
    private RoleHistory rh;
    
    public newerThan(RoleHistory rh) {
      this.rh = rh;
    }
    
    @Override
    public int compare(Container c1, Container c2) {
      int role1 = ContainerPriority.extractRole(c1);
      int role2 = ContainerPriority.extractRole(c2);
      if (role1 < role2) return -1;
      if (role1 > role2) return 1;

      NodeInstance o1 = rh.getOrCreateNodeInstance(c1), o2 = rh.getOrCreateNodeInstance(c2);
      long age = o1.getOrCreate(role1).getLastUsed();
      long age2 = o2.getOrCreate(role1).getLastUsed();

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
   * @param allocatedContainers the list of allocated containers
   * @param requested empty list of requested locations 
   * @param unrequested empty list of unrequested hosts
   */
  public synchronized void partitionRequests(RoleHistory rh, List<Container> allocatedContainers,
                                                List<Container> requested,
                                                List<Container> unrequested) {
    Collections.sort(allocatedContainers, new newerThan(rh));
    for (Container container : allocatedContainers) {
      int role = ContainerPriority.extractRole(container);
      String hostname = RoleHistoryUtils.hostnameOf(container);
      if (requests.containsKey(new OutstandingRequest(role, hostname))) {
        requested.add(container);
      } else {
        unrequested.add(container);
      }
    }
  }
  

  /**
   * Cancel all outstanding requests for a role: return the hostnames
   * of any canceled requests
   *
   * @param role role to cancel
   * @return possibly empty list of hostnames
   */
  public synchronized List<NodeInstance> cancelOutstandingRequests(int role) {
    List<NodeInstance> hosts = new ArrayList<NodeInstance>();
    Iterator<Map.Entry<OutstandingRequest,OutstandingRequest>> iterator =
      requests.entrySet().iterator();
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
  
  public synchronized List<OutstandingRequest> listOutstandingRequests() {
    return new ArrayList<OutstandingRequest>(requests.values());
  }
}
