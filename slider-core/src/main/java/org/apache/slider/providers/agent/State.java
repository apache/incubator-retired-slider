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

/** The states a component instance can be. */
public enum State {
  INIT,           // Not installed
  INSTALLING,     // Being installed
  INSTALLED,      // Installed (or stopped)
  STARTING,       // Starting
  STARTED,        // Started
  INSTALL_FAILED;  // Install failed, start failure in INSTALLED

  /**
   * Indicates whether or not it is a valid state to produce a command.
   *
   * @return true if command can be issued for this state.
   */
  public boolean canIssueCommands() {
    switch (this) {
      case INSTALLING:
      case STARTING:
      case STARTED:
        return false;
      default:
        return true;
    }
  }

  /**
   * Returns valid command in this state.
   *
   * @return command allowed in this state.
   */
  public Command getSupportedCommand() {
    switch (this) {
      case INIT:
      case INSTALL_FAILED:
        return Command.INSTALL;
      case INSTALLED:
        return Command.START;
      default:
        return Command.NOP;
    }
  }

  /**
   * Returns next state based on the command result.
   *
   * @return next state.
   */
  public State getNextState(CommandResult result) throws IllegalArgumentException {
    switch (result) {
      case IN_PROGRESS:
        if (this == State.INSTALLING || this == State.STARTING) {
          return this;
        } else {
          throw new IllegalArgumentException(result + " is not valid for " + this);
        }
      case COMPLETED:
        if (this == State.INSTALLING) {
          return State.INSTALLED;
        } else if (this == State.STARTING) {
          return State.STARTED;
        } else {
          throw new IllegalArgumentException(result + " is not valid for " + this);
        }
      case FAILED:
        if (this == State.INSTALLING) {
          return State.INSTALL_FAILED;
        } else if (this == State.STARTING) {
          return State.INSTALLED;
        } else {
          throw new IllegalArgumentException(result + " is not valid for " + this);
        }
      default:
        throw new IllegalArgumentException("Bad command result " + result);
    }
  }

  /**
   * Returns next state based on the command.
   *
   * @return next state.
   */
  public State getNextState(Command command) throws IllegalArgumentException {
    switch (command) {
      case INSTALL:
        if (this == State.INIT || this == State.INSTALL_FAILED) {
          return State.INSTALLING;
        } else {
          throw new IllegalArgumentException(command + " is not valid for " + this);
        }
      case START:
        if (this == State.INSTALLED) {
          return State.STARTING;
        } else {
          throw new IllegalArgumentException(command + " is not valid for " + this);
        }
      case NOP:
        return this;
      default:
        throw new IllegalArgumentException("Bad command " + command);
    }
  }

  public boolean couldHaveIssued(Command command) {
    if ((this == State.INSTALLING && command == Command.INSTALL) ||
        (this == State.STARTING && command == Command.START)) {
      return true;
    }
    return false;
  }
}
