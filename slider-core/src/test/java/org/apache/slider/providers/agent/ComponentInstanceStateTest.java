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
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ComponentInstanceStateTest {
  protected static final Logger log =
      LoggerFactory.getLogger(ComponentInstanceStateTest.class);
  private State[] states = new State[]{
      State.INIT, State.INSTALLING, State.INSTALLED,
      State.STARTING, State.STARTED, State.INSTALL_FAILED};

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
  public void testGetNextStateBasedOnResult() {
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
  public void testGetNextStateBasedOnCommand() {
    for (int index = 0; index < states.length; index++) {
      TestCase.assertEquals(states[index], states[index].getNextState(Command.NOP));
    }

    TestCase.assertEquals(State.INSTALLING, State.INIT.getNextState(Command.INSTALL));
    TestCase.assertEquals(State.INSTALLING, State.INSTALL_FAILED.getNextState(Command.INSTALL));
    expectExceptionOnGetNextForCommand(IllegalArgumentException.class, State.INSTALLED, Command.INSTALL);
    expectExceptionOnGetNextForCommand(IllegalArgumentException.class, State.STARTING, Command.INSTALL);
    expectExceptionOnGetNextForCommand(IllegalArgumentException.class, State.STARTED, Command.INSTALL);

    TestCase.assertEquals(State.STARTING, State.INSTALLED.getNextState(Command.START));
    expectExceptionOnGetNextForCommand(IllegalArgumentException.class, State.INIT, Command.START);
    expectExceptionOnGetNextForCommand(IllegalArgumentException.class, State.INSTALL_FAILED, Command.START);
    expectExceptionOnGetNextForCommand(IllegalArgumentException.class, State.STARTING, Command.START);
    expectExceptionOnGetNextForCommand(IllegalArgumentException.class, State.INSTALLING, Command.START);
    expectExceptionOnGetNextForCommand(IllegalArgumentException.class, State.STARTED, Command.START);
  }

  @Test
  public void validateStateTransitionNormal() {
    ComponentInstanceState componentInstanceState = new ComponentInstanceState("HBASE_MASTER", "CID_001", "AID_001");
    TestCase.assertEquals(State.INIT, componentInstanceState.getState());
    TestCase.assertEquals(true, componentInstanceState.hasPendingCommand());
    TestCase.assertEquals(Command.INSTALL, componentInstanceState.getNextCommand());
    TestCase.assertEquals(State.INIT, componentInstanceState.getState());
    componentInstanceState.commandIssued(Command.INSTALL);
    TestCase.assertEquals(State.INSTALLING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.IN_PROGRESS, Command.INSTALL);
    TestCase.assertEquals(State.INSTALLING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.COMPLETED, Command.INSTALL);
    TestCase.assertEquals(State.INSTALLED, componentInstanceState.getState());
    TestCase.assertEquals(Command.START, componentInstanceState.getNextCommand());
    componentInstanceState.commandIssued(Command.START);
    TestCase.assertEquals(State.STARTING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.IN_PROGRESS, Command.START);
    TestCase.assertEquals(State.STARTING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.COMPLETED, Command.START);
    TestCase.assertEquals(State.STARTED, componentInstanceState.getState());
  }

  @Test
  public void validateStateTransitionScenario2() {
    ComponentInstanceState componentInstanceState = new ComponentInstanceState("HBASE_MASTER", "CID_001", "AID_001");
    TestCase.assertEquals(State.INIT, componentInstanceState.getState());
    TestCase.assertEquals(true, componentInstanceState.hasPendingCommand());
    TestCase.assertEquals(Command.INSTALL, componentInstanceState.getNextCommand());
    TestCase.assertEquals(State.INIT, componentInstanceState.getState());

    componentInstanceState.commandIssued(Command.INSTALL);
    TestCase.assertEquals(State.INSTALLING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.INSTALL);
    TestCase.assertEquals(State.INSTALL_FAILED, componentInstanceState.getState());

    componentInstanceState.commandIssued(Command.INSTALL);
    TestCase.assertEquals(State.INSTALLING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.COMPLETED, Command.INSTALL);
    TestCase.assertEquals(State.INSTALLED, componentInstanceState.getState());
    TestCase.assertEquals(Command.START, componentInstanceState.getNextCommand());

    componentInstanceState.commandIssued(Command.START);
    TestCase.assertEquals(State.STARTING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.START);
    TestCase.assertEquals(State.INSTALLED, componentInstanceState.getState());

    componentInstanceState.commandIssued(Command.START);
    componentInstanceState.applyCommandResult(CommandResult.COMPLETED, Command.START);
    TestCase.assertEquals(State.STARTED, componentInstanceState.getState());
  }

  @Test
  public void tolerateMaxFailures() {
    ComponentInstanceState componentInstanceState = new ComponentInstanceState("HBASE_MASTER", "CID_001", "AID_001");
    TestCase.assertEquals(State.INIT, componentInstanceState.getState());
    TestCase.assertEquals(true, componentInstanceState.hasPendingCommand());
    TestCase.assertEquals(Command.INSTALL, componentInstanceState.getNextCommand());
    TestCase.assertEquals(State.INIT, componentInstanceState.getState());

    componentInstanceState.commandIssued(Command.INSTALL);
    TestCase.assertEquals(State.INSTALLING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.INSTALL);
    TestCase.assertEquals(State.INSTALL_FAILED, componentInstanceState.getState());

    componentInstanceState.commandIssued(Command.INSTALL);
    TestCase.assertEquals(State.INSTALLING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.INSTALL);
    TestCase.assertEquals(State.INSTALL_FAILED, componentInstanceState.getState());

    componentInstanceState.commandIssued(Command.INSTALL);
    TestCase.assertEquals(State.INSTALLING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.INSTALL);
    TestCase.assertEquals(State.INSTALL_FAILED, componentInstanceState.getState());

    try {
      componentInstanceState.commandIssued(Command.INSTALL);
      TestCase.fail("Command should not be allowed.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void tolerateFewFailureThenReset() {
    ComponentInstanceState componentInstanceState = new ComponentInstanceState("HBASE_MASTER", "CID_001", "AID_001");
    TestCase.assertEquals(State.INIT, componentInstanceState.getState());
    TestCase.assertEquals(true, componentInstanceState.hasPendingCommand());
    TestCase.assertEquals(Command.INSTALL, componentInstanceState.getNextCommand());
    TestCase.assertEquals(State.INIT, componentInstanceState.getState());

    componentInstanceState.commandIssued(Command.INSTALL);
    TestCase.assertEquals(State.INSTALLING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.INSTALL);
    TestCase.assertEquals(State.INSTALL_FAILED, componentInstanceState.getState());

    componentInstanceState.commandIssued(Command.INSTALL);
    TestCase.assertEquals(State.INSTALLING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.INSTALL);
    TestCase.assertEquals(State.INSTALL_FAILED, componentInstanceState.getState());

    componentInstanceState.commandIssued(Command.INSTALL);
    TestCase.assertEquals(State.INSTALLING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.COMPLETED, Command.INSTALL);
    TestCase.assertEquals(State.INSTALLED, componentInstanceState.getState());

    componentInstanceState.commandIssued(Command.START);
    TestCase.assertEquals(State.STARTING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.START);
    TestCase.assertEquals(State.INSTALLED, componentInstanceState.getState());

    componentInstanceState.commandIssued(Command.START);
    TestCase.assertEquals(State.STARTING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.START);
    TestCase.assertEquals(State.INSTALLED, componentInstanceState.getState());

    componentInstanceState.commandIssued(Command.START);
    TestCase.assertEquals(State.STARTING, componentInstanceState.getState());
    componentInstanceState.applyCommandResult(CommandResult.FAILED, Command.START);
    TestCase.assertEquals(State.INSTALLED, componentInstanceState.getState());

    try {
      componentInstanceState.commandIssued(Command.START);
      TestCase.fail("Command should not be allowed.");
    } catch (IllegalArgumentException e) {
    }
  }

  @Test
  public void testBadTransitions() {
    ComponentInstanceState componentInstanceState = new ComponentInstanceState("HBASE_MASTER", "CID_001", "AID_001");

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
      Class<T> expected, State state, CommandResult result) {
    try {
      state.getNextState(result);
      TestCase.fail("Must fail");
    } catch (Exception e) {
      if (!expected.isInstance(e)) {
        TestCase.fail("Unexpected exception " + e.getClass());
      }
    }
  }

  private <T extends Throwable> void expectExceptionOnGetNextForCommand(
      Class<T> expected, State state, Command command) {
    try {
      state.getNextState(command);
      TestCase.fail("Must fail");
    } catch (Exception e) {
      if (!expected.isInstance(e)) {
        TestCase.fail("Unexpected exception " + e.getClass());
      }
    }
  }
}
