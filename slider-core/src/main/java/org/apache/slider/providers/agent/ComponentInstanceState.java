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

import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.slider.providers.agent.application.metadata.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements a simple state machine for component instances. */
public class ComponentInstanceState {
  public static final Logger log =
      LoggerFactory.getLogger(ComponentInstanceState.class);
  private static int MAX_FAILURE_TOLERATED = 3;
  private static String INVALID_TRANSITION_ERROR =
      "Result %s for command %s is not expected for component %s in state %s.";

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

  private TreeMap<String, State> pkgStatuses = new TreeMap<String, State>();
  private String nextPkgToInstall;

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

  public ComponentInstanceState(String componentName,
      ContainerId containerId,
      String applicationId, TreeMap<String, State> pkgStatuses) {
    this.componentName = componentName;
    this.containerId = containerId;
    this.containerIdAsString = containerId.toString();
    this.applicationId = applicationId;
    this.containerState = ContainerState.INIT;
    this.lastHeartbeat = System.currentTimeMillis();
    this.pkgStatuses = pkgStatuses;
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
    commandIssued(command, false);
  }

  public void commandIssued(Command command, boolean isInUpgradeMode) {
    Command expected = getNextCommand(isInUpgradeMode);
    if (expected != command) {
      throw new IllegalArgumentException("Command " + command + " is not allowed in state " + state);
    }
    if (expected == Command.INSTALL_ADDON){
      //for add on packages. the pkg must be nextPkgToInstall
      State currentState = pkgStatuses.get(nextPkgToInstall);
      log.debug("commandissued: component: {} is in {}", componentName, currentState);
      State nextState = currentState.getNextState(command);
      pkgStatuses.put(nextPkgToInstall, nextState);
      log.debug("commandissued: component: {} is now in {}", componentName, nextState);
    } else {
      //for master package
      state = state.getNextState(command);
    }
  }

  public void applyCommandResult(CommandResult result, Command command, String pkg) {
    // if the heartbeat is for a package 
    // update that package's state in the component status
    // and don't bother with the master pkg
    if (pkg != null && !pkg.isEmpty()
        && !pkg.equals(Component.MASTER_PACKAGE_NAME)) {
      log.debug("this result is for component: {} pkg: {}", componentName, pkg);
      State previousPkgState = pkgStatuses.get(pkg);
      log.debug("currently component: {} pkg: {} is in state: {}", componentName, pkg, previousPkgState.toString());
      State nextPkgState = previousPkgState.getNextState(result);
      pkgStatuses.put(pkg, nextPkgState);
      log.debug("component: {} pkg: {} next state: {}", componentName, pkg, nextPkgState);
    } else {
      log.debug("this result is for component: {} master package", componentName);
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
            result.toString(), command.toString(), componentName,
            state.toString());
        log.warn(message);
        throw new IllegalStateException(message);
      }
    }
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
            result.toString(), command.toString(), componentName,
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
    return getNextCommand(false);
  }

  public Command getNextCommand(boolean isInUpgradeMode) {
    if (!hasPendingCommand()) {
      nextPkgToInstall = null;
      return Command.NOP;
    }

    log.debug("start checking for component: {} ", componentName);
    // if the master pkg is just installed, check if any add on pkg need to be
    // installed
    if (state == State.INSTALLED) {
      // first check if any pkg is install in progress, if so, wait
      for (String pkg : pkgStatuses.keySet()) {
        State pkgState = pkgStatuses.get(pkg);
        log.debug("in getNextCommand, pkg: {} is in {}", pkg, pkgState);
        if (pkgState == State.INSTALLING) {
          log.debug("in getNextCommand, pkg: {} we are issuing NOP", pkg);
          nextPkgToInstall = pkg;
          return Command.NOP;
        }
      }
      // then check if any pkg is in init, if so, install it
      for (String pkg : pkgStatuses.keySet()) {
        State pkgState = pkgStatuses.get(pkg);
        log.debug("in getNextCommand, pkg: {} is in {}", pkg, pkgState);
        if (pkgState == State.INIT) {
          log.debug("in getNextCommand, pkg: {} we are issuing install addon", pkg);
          nextPkgToInstall = pkg;
          return Command.INSTALL_ADDON;
        }
      }
    }
    nextPkgToInstall = null;
    return this.state.getSupportedCommand(isInUpgradeMode);
  }

  public State getState() {
    return state;
  }

  @VisibleForTesting
  protected void setState(State state) {
    this.state = state;
  }

  public State getTargetState() {
    return targetState;
  }

  public void setTargetState(State targetState) {
    this.targetState = targetState;
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

  public String getNextPkgToInstall() {
    return nextPkgToInstall;
  }
}
