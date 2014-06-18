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
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Test

@CompileStatic
@Slf4j
public class TestAppsThroughAgent extends AgentCommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  private static String COMMAND_LOGGER = "COMMAND_LOGGER"
  private static String APPLICATION_NAME = "agenttst"

  @After
  public void destroyCluster() {
    cleanup()
  }

  @Test
  public void testCreateFlex() throws Throwable {
    if (!AGENTTESTS_ENABLED) {
      log.info "TESTS are not run."
      return
    }

    cleanup()
    try {
      SliderShell shell = slider(EXIT_SUCCESS,
          [
              ACTION_CREATE, APPLICATION_NAME,
              ARG_IMAGE, agentTarballPath.toString(),
              ARG_TEMPLATE, APP_TEMPLATE,
              ARG_RESOURCES, APP_RESOURCE
          ])

      logShell(shell)

      ensureApplicationIsUp(APPLICATION_NAME)

      //flex
      slider(EXIT_SUCCESS,
          [
              ACTION_FLEX,
              APPLICATION_NAME,
              ARG_COMPONENT,
              COMMAND_LOGGER,
              "2"])

      // sleep till the new instance starts
      sleep(1000 * 10)

      shell = slider(EXIT_SUCCESS,
          [
              ACTION_STATUS,
              APPLICATION_NAME])

      assertComponentCount(COMMAND_LOGGER, 2, shell)

      shell = slider(EXIT_SUCCESS,
          [
              ACTION_LIST,
              APPLICATION_NAME])

      assert isAppRunning("RUNNING", shell), 'App is not running.'

      assertSuccess(shell)
    } finally {
      cleanup()
    }
  }


  public void cleanup() throws Throwable {
    log.info "Cleaning app instance, if exists, by name " + APPLICATION_NAME
    teardown(APPLICATION_NAME)

    // sleep till the instance is frozen
    sleep(1000 * 3)

    SliderShell shell = slider([
        ACTION_DESTROY,
        APPLICATION_NAME])

    if (shell.ret != 0 && shell.ret != EXIT_UNKNOWN_INSTANCE) {
      logShell(shell)
      assert fail("Old cluster either should not exist or should get destroyed.")
    }
  }


}
