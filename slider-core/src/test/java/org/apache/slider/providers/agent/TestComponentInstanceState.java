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

package org.apache.slider.providers.agent;

import junit.framework.TestCase;
import org.apache.slider.server.appmaster.model.mock.MockContainerId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestComponentInstanceState {
  protected static final Logger log =
      LoggerFactory.getLogger(TestComponentInstanceState.class);
  private State[] states = new State[]{
      State.INIT, State.INSTALLING, State.INSTALLED,
      State.STARTING, State.STARTED, State.INSTALL_FAILED};
  private final MockContainerId containerId = new MockContainerId(1);

  @Test
  public void testValidateSupportedCommands() {
    Command[] commands = new Command[]{
        Command.INSTALL, Command.NOP, Command.START,
        Command.NOP, Command.NOP, Command.INSTALL};

    for (int index = 0; index < states.length; index++) {
      TestCase.assertEquals(commands[index], states[index].getSupportedCommand());
    }
  }

  @Test
  public void testGetNextStateBasedOnResult() throws Exception {
    TestCase.assertEquals(State.INSTALLING, State.INSTALLING.getNextState(CommandResult.IN_PROGRESS));
    TestCase.assertEquals(State.STARTING, State.STARTING.getNextState(CommandResult.IN_PROGRESS));
    expectExceptionOnGetNextForResult(IllegalArgumentException.class, State.INIT, CommandResult.IN_PROGRESS);
    expectExceptionOnGetNextForResult(IllegalArgumentException.class, State.INSTALLED, CommandResult.IN_PROGRESS);
    expectExceptionOnGetNextForResult(IllegalArgumentException.class, State.STARTED, CommandResult.IN_PROGRESS);
    expectExceptionOnGetNextForResult(IllegalArgumentException.class, State.INSTALL_FAILED, CommandResult.IN_PROGRESS);

    TestCase.assertEquals(State.INSTALLED, State.INSTALLING.getNextState(CommandResult.COMPLETED));
    TestCase.assertEquals(State.STARTED, State.STARTING.getNextState(CommandResult.COMPLETED));
    expectExceptionOnGetNextForResult(IllegalArgumentException.class, State.INIT, CommandResult.COMPLETED);
    expectExceptionOnGetNextForResult(IllegalArgumentException.class, State.INSTALLED, CommandResult.COMPLETED);
    expectExceptionOnGetNextForResult(IllegalArgumentException.class, State.STARTED, CommandResult.COMPLETED);
    expectExceptionOnGetNextForResult(IllegalArgumentException.class, State.INSTALL_FAILED, CommandResult.COMPLETED);

    TestCase.assertEquals(State.INSTALL_FAILED, State.INSTALLING.getNextState(CommandResult.FAILED));
    TestCase.assertEquals(State.INSTALLED, State.STARTING.getNextState(CommandResult.FAILED));
    expectExceptionOnGetNextForResult(IllegalArgumentException.class, State.INIT, CommandResult.FAILED);
    expectExceptionOnGetNextForResult(IllegalArgumentException.class, State.INSTALLED, CommandResult.FAILED);
    expectExceptionOnGetNextForResult(IllegalArgumentException.class, State.STARTED, CommandResult.FAILED);
    expectExceptionOnGetNextForResult(IllegalArgumentException.class, State.INSTALL_FAILED, CommandResult.FAILED);
  }

  @Test
  public void testGetNextStateBasedOnCommand() throws Exception {
    for (State state : states) {
      TestCase.assertEquals(state, state.getNextState(Command.NOP));
    }

    TestCase.assertEquals(State.INSTALLING, State.INIT.getNextState(Command.INSTALL));
    TestCase.assertEquals(State.INSTALLING, State.INSTALL_FAILED.getNextState(Command.INSTALL));
    expectIllegalArgumentException(State.INSTALLED, Command.INSTALL);
    expectIllegalArgumentException(State.STARTING, Command.INSTALL);
    expectIllegalArgumentException(State.STARTED, Command.INSTALL);

    TestCase.assertEquals(State.STARTING, State.INSTALLED.getNextState(Command.START));
    expectIllegalArgumentException(State.INIT, Command.START);
    expectIllegalArgumentException(State.INSTALL_FAILED, Command.START);
    expectIllegalArgumentException(State.STARTING, Command.START);
    expectIllegalArgumentException(State.INSTALLING, Command.START);
    expectIllegalArgumentException(State.STARTED, Command.START);
  }

  protected void expectIllegalArgumentException(State state, Command command) throws
      Exception {
    expectExceptionOnGetNextForCommand(IllegalArgumentException.class,
        state, command);
  }

  @Test
  public void validateStateTransitionNormal() {
    ComponentInstanceState componentInstanceState = new ComponentInstanceState("HBASE_MASTER", containerId, "AID_001");
    assertInState(State.INIT, componentInstanceState);
    TestCase.assertEquals(true, componentInstanceState.hasPendingCommand());
    TestCase.assertEquals(Command.INSTALL, componentInstanceState.getNextCommand());
    assertInState(State.INIT, componentInstanceState);
    componentInstanceState.commandIssued(Command.INSTALL);
    assertInState(State.INSTALLING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.IN_PROGRESS, Command.INSTALL);
    assertInState(State.INSTALLING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.COMPLETED, Command.INSTALL);
    assertInState(State.INSTALLED, componentInstanceState);
    TestCase.assertEquals(Command.START, componentInstanceState.getNextCommand());
    componentInstanceState.commandIssued(Command.START);
    assertInState(State.STARTING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.IN_PROGRESS, Command.START);
    assertInState(State.STARTING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.COMPLETED, Command.START);
    assertInState(State.STARTED, componentInstanceState);
  }

  protected void assertInState(State state,
      ComponentInstanceState componentInstanceState) {
    TestCase.assertEquals(state, componentInstanceState.getState());
  }

  @Test
  public void validateStateTransitionScenario2() {
    ComponentInstanceState componentInstanceState = new ComponentInstanceState("HBASE_MASTER", containerId, "AID_001");
    assertInState(State.INIT, componentInstanceState);
    TestCase.assertEquals(true, componentInstanceState.hasPendingCommand());
    TestCase.assertEquals(Command.INSTALL, componentInstanceState.getNextCommand());
    assertInState(State.INIT, componentInstanceState);

    componentInstanceState.commandIssued(Command.INSTALL);
    assertInState(State.INSTALLING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.INSTALL);
    assertInState(State.INSTALL_FAILED, componentInstanceState);

    componentInstanceState.commandIssued(Command.INSTALL);
    assertInState(State.INSTALLING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.COMPLETED, Command.INSTALL);
    assertInState(State.INSTALLED, componentInstanceState);
    TestCase.assertEquals(Command.START, componentInstanceState.getNextCommand());

    componentInstanceState.commandIssued(Command.START);
    assertInState(State.STARTING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.START);
    assertInState(State.INSTALLED, componentInstanceState);

    componentInstanceState.commandIssued(Command.START);
    componentInstanceState.applyCommandResult(CommandResult.COMPLETED, Command.START);
    assertInState(State.STARTED, componentInstanceState);
  }

  @Test
  public void tolerateMaxFailures() {
    ComponentInstanceState componentInstanceState = new ComponentInstanceState("HBASE_MASTER", containerId, "AID_001");
    assertInState(State.INIT, componentInstanceState);
    TestCase.assertEquals(true, componentInstanceState.hasPendingCommand());
    TestCase.assertEquals(Command.INSTALL, componentInstanceState.getNextCommand());
    assertInState(State.INIT, componentInstanceState);

    componentInstanceState.commandIssued(Command.INSTALL);
    assertInState(State.INSTALLING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.INSTALL);
    assertInState(State.INSTALL_FAILED, componentInstanceState);

    componentInstanceState.commandIssued(Command.INSTALL);
    assertInState(State.INSTALLING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.INSTALL);
    assertInState(State.INSTALL_FAILED, componentInstanceState);

    componentInstanceState.commandIssued(Command.INSTALL);
    assertInState(State.INSTALLING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.INSTALL);
    assertInState(State.INSTALL_FAILED, componentInstanceState);

    try {
      componentInstanceState.commandIssued(Command.INSTALL);
      TestCase.fail("Command should not be allowed.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void tolerateFewFailureThenReset() {
    ComponentInstanceState componentInstanceState = new ComponentInstanceState("HBASE_MASTER", containerId, "AID_001");
    assertInState(State.INIT, componentInstanceState);
    TestCase.assertEquals(true, componentInstanceState.hasPendingCommand());
    TestCase.assertEquals(Command.INSTALL, componentInstanceState.getNextCommand());
    assertInState(State.INIT, componentInstanceState);

    componentInstanceState.commandIssued(Command.INSTALL);
    assertInState(State.INSTALLING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.INSTALL);
    assertInState(State.INSTALL_FAILED, componentInstanceState);

    componentInstanceState.commandIssued(Command.INSTALL);
    assertInState(State.INSTALLING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.INSTALL);
    assertInState(State.INSTALL_FAILED, componentInstanceState);

    componentInstanceState.commandIssued(Command.INSTALL);
    assertInState(State.INSTALLING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.COMPLETED, Command.INSTALL);
    assertInState(State.INSTALLED, componentInstanceState);

    componentInstanceState.commandIssued(Command.START);
    assertInState(State.STARTING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.START);
    assertInState(State.INSTALLED, componentInstanceState);

    componentInstanceState.commandIssued(Command.START);
    assertInState(State.STARTING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.START);
    assertInState(State.INSTALLED, componentInstanceState);

    componentInstanceState.commandIssued(Command.START);
    assertInState(State.STARTING, componentInstanceState);
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.START);
    assertInState(State.INSTALLED, componentInstanceState);

    try {
      componentInstanceState.commandIssued(Command.START);
      TestCase.fail("Command should not be allowed.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testBadTransitions() {
    ComponentInstanceState componentInstanceState = new ComponentInstanceState("HBASE_MASTER", containerId, "AID_001");

    try {
      componentInstanceState.commandIssued(Command.START);
      TestCase.fail("Command should not be allowed.");
    } catch (IllegalArgumentException e) {
    }

    componentInstanceState.commandIssued(Command.INSTALL);
    try {
      componentInstanceState.commandIssued(Command.START);
      TestCase.fail("Command should not be allowed.");
    } catch (IllegalArgumentException e) {
    }

    try {
      componentInstanceState.applyCommandResult(CommandResult.COMPLETED, Command.START);
      TestCase.fail("Command should not be allowed.");
    } catch (IllegalStateException e) {
    }

    try {
      componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.START);
      TestCase.fail("Command should not be allowed.");
    } catch (IllegalStateException e) {
    }

    try {
      componentInstanceState.commandIssued(Command.INSTALL);
      TestCase.fail("Command should not be allowed.");
    } catch (IllegalArgumentException e) {
    }

    componentInstanceState.applyCommandResult(CommandResult.COMPLETED, Command.INSTALL);

    try {
      componentInstanceState.commandIssued(Command.INSTALL);
      TestCase.fail("Command should not be allowed.");
    } catch (IllegalArgumentException e) {
    }

    try {
      componentInstanceState.commandIssued(Command.NOP);
      TestCase.fail("Command should not be allowed.");
    } catch (IllegalArgumentException e) {
    }
  }

  private <T extends Throwable> void expectExceptionOnGetNextForResult(
      Class<T> expected, State state, CommandResult result) throws Exception {
    try {
      state.getNextState(result);
      TestCase.fail("Must fail");
    } catch (Exception e) {
      if (!expected.isInstance(e)) {
        throw e;
      }
    }
  }

  private <T extends Throwable> void expectExceptionOnGetNextForCommand(
      Class<T> expected, State state, Command command) throws Exception {
    try {
      state.getNextState(command);
      TestCase.fail("Must fail");
    } catch (Exception e) {
      if (!expected.isInstance(e)) {
        throw e;
      }
    }
  }
}
