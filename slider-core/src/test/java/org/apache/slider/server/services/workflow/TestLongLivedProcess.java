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

package org.apache.slider.server.services.workflow;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * Test the long lived process by executing a command that works and a command
 * that fails
 */
public class TestLongLivedProcess extends WorkflowServiceTestBase implements
    LongLivedProcessLifecycleEvent {
  private static final Logger
      log = LoggerFactory.getLogger(TestLongLivedProcess.class);

  private static final Logger
      processLog =
      LoggerFactory.getLogger("org.apache.hadoop.services.workflow.Process");


  private LongLivedProcess process;
  private File testDir = new File("target");
  private ProcessCommandFactory commandFactory;
  private volatile boolean started, stopped;
  private volatile int exitCode;

  @Before
  public void setupProcesses() {
    commandFactory = ProcessCommandFactory.createProcessCommandFactory();
  }

  @After
  public void stopProcesses() {
    if (process != null) {
      process.stop();
    }
  }

  @Test
  public void testLs() throws Throwable {

    initProcess(commandFactory.ls(testDir));
    process.start();
    //in-thread wait
    process.run();

    //here stopped
    assertTrue("process start callback not received", started);
    assertTrue("process stop callback not received", stopped);
    assertEquals(0, process.getExitCode().intValue());

    assertStringInOutput("test-classes", getFinalOutput());
  }

  @Test
  public void testEcho() throws Throwable {

    String echoText = "hello, world";
    initProcess(commandFactory.echo(echoText));
    process.start();
    //in-thread wait
    process.run();

    //here stopped
    assertTrue("process stop callback not received", stopped);
    assertEquals(0, process.getExitCode().intValue());
    assertStringInOutput(echoText, getFinalOutput());
  }

  @Test
  public void testSetenv() throws Throwable {

    String var = "TEST_RUN";
    String val = "TEST-RUN-ENV-VALUE";
    String echoText = "${TEST_RUN}";
    initProcess(commandFactory.env());
    process.setEnv(var, val);
    process.start();
    //in-thread wait
    process.run();

    //here stopped
    assertTrue("process stop callback not received", stopped);
    assertEquals(0, process.getExitCode().intValue());
    assertStringInOutput(val, getFinalOutput());
  }

  /**
   * Get the final output. includes a quick sleep for the tail output
   * @return the last output
   * @throws InterruptedException
   */
  private List<String> getFinalOutput() throws InterruptedException {
    return process.getRecentOutput();
  }

  public void assertStringInOutput(String text, List<String> output) {
    boolean found = false;
    StringBuilder builder = new StringBuilder();
    for (String s : output) {
      builder.append(s).append('\n');
      if (s.contains(text)) {
        found = true;
        break;
      }
    }

    if (!found) {
      String message =
          "Text \"" + text + "\" not found in " + output.size() + " lines\n";
      fail(message + builder.toString());
    }

  }


  private LongLivedProcess initProcess(List<String> commands) {
    process = new LongLivedProcess(name.getMethodName(), log, commands);
    process.setLifecycleCallback(this);
    return process;
  }

  /**
   * Handler for callback events on the process
   */


  @Override
  public void onProcessStarted(LongLivedProcess process) {
    started = true;
  }

  /**
   * Handler for callback events on the process
   */
  @Override
  public void onProcessExited(LongLivedProcess process, int exitCode) {
    this.exitCode = exitCode;
    stopped = true;
  }
}
