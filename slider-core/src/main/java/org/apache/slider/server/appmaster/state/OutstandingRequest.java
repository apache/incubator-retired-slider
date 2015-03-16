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


import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tracks an outstanding request. This is used to correlate an allocation response
 * with the node and role used in the request.
 * <p>
 * The node identifier may be null -which indicates that a request was made without
 * a specific target node
 * <p>
 * Equality and the hash code are based <i>only</i> on the role and hostname,
 * which are fixed in the constructor. This means that a simple 
 * instance constructed with (role, hostname) can be used to look up
 * a complete request instance in the {@link OutstandingRequestTracker} map
 */
public final class OutstandingRequest {
  protected static final Logger log =
    LoggerFactory.getLogger(OutstandingRequest.class);

  /**
   * requested role
   */
  public final int roleId;

  /**
   * Node the request is for -may be null
   */
  public final NodeInstance node;
  
  /**
   * hostname -will be null if node==null
   */
  public final String hostname;

  /**
   * Requested time in millis.
   * <p>
   * Only valid after {@link #buildContainerRequest(Resource, RoleStatus, long, String)}
   */
  public AMRMClient.ContainerRequest issuedRequest;
  
  /**
   * Requested time in millis.
   * <p>
   * Only valid after {@link #buildContainerRequest(Resource, RoleStatus, long, String)}
   */
  public long requestedTimeMillis;

  /**
   * Time in millis after which escalation should be triggered..
   * <p>
   * Only valid after {@link #buildContainerRequest(Resource, RoleStatus, long, String)}
   */
  public long escalationTimeoutMillis;

  /**
   * Has the placement request been escalated?
   */
  public boolean escalated;
  
  /**
   * Create a request
   * @param roleId role
   * @param node node -can be null
   */
  public OutstandingRequest(int roleId,
                            NodeInstance node) {
    this.roleId = roleId;
    this.node = node;
    this.hostname = node != null ? node.hostname : null;
  }

  /**
   * Create an outsanding request with the given role and hostname
   * Important: this is useful only for map lookups -the other constructor
   * with the NodeInstance parameter is needed to generate node-specific
   * container requests
   * @param roleId role
   * @param hostname hostname
   */
  public OutstandingRequest(int roleId, String hostname) {
    this.node = null;
    this.roleId = roleId;
    this.hostname = hostname;
  }

  /**
   * Is the request located in the cluster, that is: does it have a node.
   * @return
   */
  public boolean isLocated() {
    return node != null;
  }
  
  /**
   * Build a container request.
   * If the request has an address, it is set in the container request
   * (with a flag to enable relaxed priorities).
   * <p>
   * This operation sets the requested time flag, used for tracking timeouts
   * on outstanding requests
   * @param resource resource
   * @param role role
   * @param time time in millis to record as request time
   * @param labelExpression label to satisfy
   * @return the request to raise
   */
  public AMRMClient.ContainerRequest buildContainerRequest(
      Resource resource, RoleStatus role, long time, String labelExpression) {
    String[] hosts;
    boolean relaxLocality;
    requestedTimeMillis = time;
    escalationTimeoutMillis = time + role.getPlacementTimeoutSeconds() * 1000;
    boolean usePlacementHistory = role.isStrictPlacement();
    if (!usePlacementHistory) {
      // If strict placement does not mandate using placement then check
      // that the recent failures on this node is not higher than threshold
      if (node != null) {
        int numFailuresOnLastHost = node.get(role.getKey()).getFailedRecently();
        usePlacementHistory = numFailuresOnLastHost <= role.getNodeFailureThreshold();
        if(!usePlacementHistory) {
          log.info("Recent node failures {} is higher than threshold {}. Dropping host {} from preference.",
                   numFailuresOnLastHost, role.getNodeFailureThreshold(), node.hostname);
        }
      }
    }

    if (node != null && usePlacementHistory) {
      hosts = new String[1];
      hosts[0] = node.hostname;
      relaxLocality = !role.isStrictPlacement();
      // tell the node it is in play
      node.getOrCreate(roleId);
      log.info("Submitting request for container on {}", hosts[0]);
      escalated = false;
    } else {
      // the placement is implicitly escalated.
      escalated = true;
      hosts = null;
      relaxLocality = true;
    }
    Priority pri = ContainerPriority.createPriority(roleId, !relaxLocality);
    issuedRequest = new AMRMClient.ContainerRequest(resource,
                                      hosts,
                                      null,
                                      pri,
                                      relaxLocality,
                                      labelExpression);

    return issuedRequest;
  }


  /**
   * Build an escalated container request, updating {@link #issuedRequest} with
   * the new value.
   * @return the new container request, which has the same resource and label requirements
   * as the original one, and the same host, but: relaxed placement, and a changed priority
   * so as to place it into the relaxed list.
   */
  public AMRMClient.ContainerRequest escalate() {
    escalated = true;
    Preconditions.checkNotNull(issuedRequest, "issued request");
    Priority pri = ContainerPriority.createPriority(roleId, true);
    String[] nodes;
    List<String> issuedRequestNodes = issuedRequest.getNodes();
    if (issuedRequestNodes != null) {
      nodes = issuedRequestNodes.toArray(new String[issuedRequestNodes.size()]);
    } else {
      nodes = null;
    }


    AMRMClient.ContainerRequest newRequest =
        new AMRMClient.ContainerRequest(issuedRequest.getCapability(),
            nodes,
            null,
            pri,
            true,
            issuedRequest.getNodeLabelExpression());
    issuedRequest = newRequest;
    return issuedRequest;
  }
      
  /**
   * Mark the request as completed (or canceled).
   */
  public void completed() {
    assert node != null : "null node";
    node.getOrCreate(roleId).requestCompleted();
  }

  /**
   * Query to see if the request is ready to be escalated
   * @param time time to check against
   * @return true if escalation should begin
   */
  public boolean shouldEscalate(long time) {
    return !escalated && escalationTimeoutMillis < time;
  }
  
  /**
   * Equality is on hostname and role
   * @param o other
   * @return true on a match
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OutstandingRequest request = (OutstandingRequest) o;

    if (roleId != request.roleId) {
      return false;
    }
    if (hostname != null
        ? !hostname.equals(request.hostname)
        : request.hostname != null) {
      return false;
    }
    return true;
  }

  /**
   * hash on hostname and role
   * @return hash code
   */
  @Override
  public int hashCode() {
    int result = roleId;
    result = 31 * result + (hostname != null ? hostname.hashCode() : 0);
    return result;
  }
  
  @Override
  public String toString() {
    final StringBuilder sb =
      new StringBuilder("OutstandingRequest{");
    sb.append("roleId=").append(roleId);
    sb.append(", node='").append(node).append('\'');
    sb.append(", requestedTime=").append(requestedTimeMillis);
    sb.append('}');
    return sb.toString();
  }
}
