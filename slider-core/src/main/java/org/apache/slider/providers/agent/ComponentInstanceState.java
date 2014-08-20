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

package org.apache.slider.providers.agent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements a simple state machine for component instances. */
public class ComponentInstanceState {
  public static final Logger log =
      LoggerFactory.getLogger(ComponentInstanceState.class);
  private static int MAX_FAILURE_TOLERATED = 3;
  private static String INVALID_TRANSITION_ERROR =
      "Result {0} for command {1} is not expected for component {2} in state {3}.";

  private final String componentName;
  private final ContainerId containerId;
  private final String containerIdAsString;
  private final String applicationId;
  private State state = State.INIT;
  private State targetState = State.STARTED;
  private int failuresSeen = 0;
  private Boolean configReported = false;
  private long lastHeartbeat = 0;
  private ContainerState containerState;

  public ComponentInstanceState(String componentName,
      ContainerId containerId,
      String applicationId) {
    this.componentName = componentName;
    this.containerId = containerId;
    this.containerIdAsString = containerId.toString();
    this.applicationId = applicationId;
    this.containerState = ContainerState.INIT;
    this.lastHeartbeat = System.currentTimeMillis();
  }

  public String getComponentName() {
    return componentName;
  }

  public Boolean getConfigReported() {
    return configReported;
  }

  public void setConfigReported(Boolean configReported) {
    this.configReported = configReported;
  }

  public ContainerState getContainerState() {
    return containerState;
  }

  public void setContainerState(ContainerState containerState) {
    this.containerState = containerState;
  }

  public long getLastHeartbeat() {
    return lastHeartbeat;
  }

  /**
   * Update the heartbeat, and change container state
   * to mark as healthy if appropriate
   * @param heartbeatTime last time the heartbeat was seen
   * @return the current container state
   */
  public ContainerState heartbeat(long heartbeatTime) {
    this.lastHeartbeat = heartbeatTime;
    if(containerState == ContainerState.UNHEALTHY ||
       containerState == ContainerState.INIT) {
      containerState = ContainerState.HEALTHY;
    }
    return containerState;
  }
  

  public ContainerId getContainerId() {
    return containerId;
  }

  public void commandIssued(Command command) {
    Command expected = getNextCommand();
    if (expected != command) {
      throw new IllegalArgumentException("Command " + command + " is not allowed in state " + state);
    }
    state = state.getNextState(command);
  }

  public void applyCommandResult(CommandResult result, Command command) {
    if (!this.state.couldHaveIssued(command)) {
      throw new IllegalStateException("Invalid command " + command + " for state " + this.state);
    }

    try {
      if (result == CommandResult.FAILED) {
        failuresSeen++;
      } else if (result == CommandResult.COMPLETED) {
        failuresSeen = 0;
      }
      state = state.getNextState(result);
    } catch (IllegalArgumentException e) {
      String message = String.format(INVALID_TRANSITION_ERROR,
                                     result.toString(),
                                     command.toString(),
                                     componentName,
                                     state.toString());
      log.warn(message);
      throw new IllegalStateException(message);
    }
  }

  public boolean hasPendingCommand() {
    if (state.canIssueCommands() &&
        state != targetState &&
        failuresSeen < MAX_FAILURE_TOLERATED) {
      return true;
    }

    return false;
  }

  public Command getNextCommand() {
    if (!hasPendingCommand()) {
      return Command.NOP;
    }

    return this.state.getSupportedCommand();
  }

  public State getState() {
    return state;
  }

  @VisibleForTesting
  protected void setState(State state) {
    this.state = state;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode ^ (componentName != null ? componentName.hashCode() : 0);
    hashCode = hashCode ^ (containerIdAsString != null ? containerIdAsString.hashCode() : 0);
    hashCode = hashCode ^ (applicationId != null ? applicationId.hashCode() : 0);
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    ComponentInstanceState that = (ComponentInstanceState) o;

    if (this.componentName != null ?
        !this.componentName.equals(that.componentName) : this.componentName != null) {
      return false;
    }

    if (this.containerIdAsString != null ?
        !this.containerIdAsString.equals(that.containerIdAsString) : this.containerIdAsString != null) {
      return false;
    }

    if (this.applicationId != null ?
        !this.applicationId.equals(that.applicationId) : this.applicationId != null) {
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("ComponentInstanceState{");
    sb.append("containerIdAsString='").append(containerIdAsString).append('\'');
    sb.append(", state=").append(state);
    sb.append(", failuresSeen=").append(failuresSeen);
    sb.append(", lastHeartbeat=").append(lastHeartbeat);
    sb.append(", containerState=").append(containerState);
    sb.append(", componentName='").append(componentName).append('\'');
    sb.append('}');
    return sb.toString();
  }
}
