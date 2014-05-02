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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements a simple state machine for component instances. */
public class ComponentInstanceState {
  public static final Logger log =
      LoggerFactory.getLogger(ComponentInstanceState.class);
  private static int MAX_FAILURE_TOLERATED = 3;
  private static String INVALID_TRANSITION_ERROR =
      "Result {0} for command {1} is not expected for component {2} in state {3}.";
  private State state = State.INIT;
  private State targetState = State.STARTED;
  private int failuresSeen = 0;
  private final String compName;
  private final String containerId;
  private final String applicationId;


  public ComponentInstanceState(String compName,
                                String containerId,
                                String applicationId) {
    this.compName = compName;
    this.containerId = containerId;
    this.applicationId = applicationId;
  }

  public void commandIssued(Command command) {
    Command expected = getNextCommand();
    if (expected != command) {
      throw new IllegalArgumentException("Command " + command + " is not allowed is state " + state);
    }
    this.state = this.state.getNextState(command);
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
      this.state = this.state.getNextState(result);
    } catch (IllegalArgumentException e) {
      String message = String.format(INVALID_TRANSITION_ERROR,
                                     result.toString(),
                                     command.toString(),
                                     compName,
                                     state.toString());
      log.warn(message);
      throw new IllegalStateException(message);
    }
  }

  public boolean hasPendingCommand() {
    if (this.state.canIssueCommands() &&
        this.state != this.targetState &&
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

    hashCode = hashCode ^ (compName != null ? compName.hashCode() : 0);
    hashCode = hashCode ^ (containerId != null ? containerId.hashCode() : 0);
    hashCode = hashCode ^ (applicationId != null ? applicationId.hashCode() : 0);
    return hashCode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;

    if (o == null || getClass() != o.getClass()) return false;

    ComponentInstanceState that = (ComponentInstanceState) o;

    if (this.compName != null ?
        !this.compName.equals(that.compName) : this.compName != null) {
      return false;
    }

    if (this.containerId != null ?
        !this.containerId.equals(that.containerId) : this.containerId != null) {
      return false;
    }

    if (this.applicationId != null ?
        !this.applicationId.equals(that.applicationId) : this.applicationId != null) {
      return false;
    }

    return true;
  }
}
