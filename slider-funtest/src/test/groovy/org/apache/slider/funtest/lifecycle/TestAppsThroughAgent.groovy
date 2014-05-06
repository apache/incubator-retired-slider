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

package org.apache.slider.funtest.lifecycle

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.CommandTestBase
import org.apache.slider.funtest.framework.SliderShell
import org.junit.Test

import static org.apache.slider.common.SliderExitCodes.EXIT_UNKNOWN_INSTANCE

@CompileStatic
@Slf4j
public class TestAppsThroughAgent extends CommandTestBase {

  private static String COMMAND_LOGGER = "COMMAND_LOGGER"
  private static String APPLICATION_NAME = "agenttst"
  private static String APP_RESOURCE = "../slider-core/src/test/app_packages/test_command_log/resources.json"
  private static String APP_TEMPLATE = "../slider-core/src/test/app_packages/test_command_log/appConfig.json"

  @Test
  public void testUsage() throws Throwable {
    SliderShell shell = slider(0, [SliderActions.ACTION_USAGE])
    assertSuccess(shell)
  }

  @Test
  public void testCreateFlexHBase() throws Throwable {
    if (!AGENTTESTS_ENABLED) {
      log.info "TESTS are not run."
      return
    }

    cleanup()
    try {
      SliderShell shell = slider(0, [
          SliderActions.ACTION_CREATE,
          TestAppsThroughAgent.APPLICATION_NAME,
          Arguments.ARG_IMAGE,
          AGENT_PKG,
          Arguments.ARG_TEMPLATE,
          TestAppsThroughAgent.APP_TEMPLATE,
          Arguments.ARG_RESOURCES,
          TestAppsThroughAgent.APP_RESOURCE])

      logShell(shell)

      assertSuccess(shell)

      int attemptCount = 0
      while (attemptCount < 10) {
        shell = slider(0, [
            SliderActions.ACTION_LIST,
            TestAppsThroughAgent.APPLICATION_NAME])

        if (isAppRunning("RUNNING", shell)) {
          break
        }

        attemptCount++
        assert attemptCount != 10, 'Application did not start, aborting test.'

        sleep(1000 * 5)
      }

      //flex
      slider(0, [
          SliderActions.ACTION_FLEX,
          TestAppsThroughAgent.APPLICATION_NAME,
          Arguments.ARG_COMPONENT,
          COMMAND_LOGGER,
          "2"])

      // sleep till the new instance starts
      sleep(1000 * 10)

      shell = slider(0, [
          SliderActions.ACTION_STATUS,
          TestAppsThroughAgent.APPLICATION_NAME])

      assertComponentCount(COMMAND_LOGGER, 2, shell)

      shell = slider(0, [
          SliderActions.ACTION_LIST,
          TestAppsThroughAgent.APPLICATION_NAME])

      assert isAppRunning("RUNNING", shell), 'App is not running.'

      assertSuccess(shell)
    } finally {
      cleanup()
    }
  }

  public void logShell(SliderShell shell) {
    for (String str in shell.out) {
      log.info str
    }
  }


  public void assertComponentCount(String component, int count, SliderShell shell) {
    log.info("Asserting component count.")
    String entry = findLineEntry(shell, ["instances", component] as String[])
    log.info(entry)
    assert entry != null
    int instanceCount = 0
    int index = entry.indexOf("container_")
    while (index != -1) {
      instanceCount++;
      index = entry.indexOf("container_", index + 1)
    }

    assert instanceCount == count, 'Instance count for component did not match expected. Parsed: ' + entry
  }

  public String findLineEntry(SliderShell shell, String[] locators) {
    int index = 0;
    for (String str in shell.out) {
      if (str.contains("\"" + locators[index] + "\"")) {
        if (locators.size() == index + 1) {
          return str;
        } else {
          index++;
        }
      }
    }

    return null;
  }

  public boolean isAppRunning(String text, SliderShell shell) {
    boolean exists = false
    for (String str in shell.out) {
      if (str.contains(text)) {
        exists = true
      }
    }

    return exists
  }

  public void cleanup() throws Throwable {
    log.info "Cleaning app instance, if exists, by name " + TestAppsThroughAgent.APPLICATION_NAME
    SliderShell shell = slider([
        SliderActions.ACTION_FREEZE,
        TestAppsThroughAgent.APPLICATION_NAME])

    if (shell.ret != 0 && shell.ret != EXIT_UNKNOWN_INSTANCE) {
      logShell(shell)
      assert fail("Old cluster either should not exist or should get frozen.")
    }

    // sleep till the instance is frozen
    sleep(1000 * 5)

    shell = slider([
        SliderActions.ACTION_DESTROY,
        TestAppsThroughAgent.APPLICATION_NAME])

    if (shell.ret != 0 && shell.ret != EXIT_UNKNOWN_INSTANCE) {
      logShell(shell)
      assert fail("Old cluster either should not exist or should get destroyed.")
    }
  }


}
