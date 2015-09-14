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

package org.apache.slider.server.appmaster.operations;

import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.slider.server.appmaster.state.ContainerPriority;
import org.apache.slider.server.appmaster.state.NodeInstance;
import org.apache.slider.server.appmaster.state.RoleHistory;
import org.apache.slider.server.appmaster.state.RoleStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A container request operation
 */
public class ContainerRequestOperation extends AbstractRMOperation {
  protected static final Logger log =
  LoggerFactory.getLogger(ContainerRequestOperation.class);

  private final AMRMClient.ContainerRequest request;
  private List<String> blacklistAdditions;
  private List<String> blacklistRemovals;

  /**
   * Build a request
   * @param request request to use
   * @param role role (or null)
   * @param activeNodesForRole list of active nodes for this role. Must be set if role is not null.
   * @param failedNodesForRole list of failed nodes for this role. Must be set if role is not null.
   */
  public ContainerRequestOperation(AMRMClient.ContainerRequest request,
      RoleStatus role, List<NodeInstance> activeNodesForRole,
      List<String> failedNodesForRole) {
    blacklistAdditions = new ArrayList<>();
    blacklistRemovals = new ArrayList<>();
    if (role != null) {
      Preconditions.checkArgument(activeNodesForRole != null, "Null activeNodesForRole");
      Preconditions.checkArgument(failedNodesForRole != null, "Null failedNodesForRole");
      log.info("ContainerRequestOperation(): Role: {} , Request {}", role.getName(), request);
      if (role.isAntiAffinePlacement()) {
        for (NodeInstance nit1 : activeNodesForRole) {
          log.info("ContainerRequestOperation(): add to blacklist nodes - Role: {},  Node: {}",
              role.getName(), nit1.hostname);
          blacklistAdditions.add(nit1.hostname);
        }
      }
      blacklistAdditions.addAll(failedNodesForRole);
    }
    this.request = request;
  }

  /**
   * Create a request with no blacklisting/affinity information
   *
   * @param request request to issue
   */
  public ContainerRequestOperation(AMRMClient.ContainerRequest request) {
    this(request, null, null, null);
  }

  /**
   * Get the underlying request
   * @return
   */
  public AMRMClient.ContainerRequest getRequest() {
    return request;
  }

  /**
   * Get the current blacklist additions
   * @return the list of additions
   */
  public List<String> getBlacklistAdditions() {
    return blacklistAdditions;
  }

  /**
   * get the current blacklist removals
   * @return the list of removals
   */
  public List<String> getBlacklistRemovals() {
    return blacklistRemovals;
  }

  @Override
  public void execute(RMOperationHandlerActions handler) {
    handler.updateBlacklist(blacklistAdditions, blacklistRemovals);
    handler.addContainerRequest(request);
  }

  @Override
  public String toString() {
    return "request container for " + ContainerPriority.toString(request.getPriority());
  }
}
