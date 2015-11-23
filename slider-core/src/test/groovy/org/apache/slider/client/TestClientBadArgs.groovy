/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.client

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.core.exceptions.BadCommandArgumentsException
import org.apache.slider.core.exceptions.ErrorStrings
import org.apache.slider.core.exceptions.UsageException
import org.apache.slider.core.main.ServiceLauncherBaseTest
import org.junit.Test

/**
 * Test the argument parsing/validation logic
 */
@CompileStatic
@Slf4j
class TestClientBadArgs extends ServiceLauncherBaseTest {

  @Test
  public void testNoAction() throws Throwable {
    launchExpectingException(SliderClient,
                             createTestConfig(),
                             "Usage: slider COMMAND",
                             [])

  }

  @Test
  public void testUnknownAction() throws Throwable {
    launchExpectingException(SliderClient,
                             createTestConfig(),
                             "not-a-known-action",
                             ["not-a-known-action"])
  }
  
  @Test
  public void testActionWithoutOptions() throws Throwable {
    launchExpectingException(SliderClient,
                             createTestConfig(),
                             "Usage: slider build <application>",
                             [SliderActions.ACTION_BUILD])
  }

  @Test
  public void testActionWithoutEnoughArgs() throws Throwable {
    launchExpectingException(SliderClient,
                             createTestConfig(),
                             ErrorStrings.ERROR_NOT_ENOUGH_ARGUMENTS,
                             [SliderActions.ACTION_THAW])
  }

  @Test
  public void testActionWithTooManyArgs() throws Throwable {
    launchExpectingException(SliderClient,
                             createTestConfig(),
                             ErrorStrings.ERROR_TOO_MANY_ARGUMENTS,
                             [SliderActions.ACTION_HELP,
                             "hello, world"])
  }

  @Test
  public void testBadImageArg() throws Throwable {
    launchExpectingException(SliderClient,
                             createTestConfig(),
                             "Unknown option: --image",
                            [SliderActions.ACTION_HELP,
                             Arguments.ARG_IMAGE])
  }

  @Test
  public void testRegistryUsage() throws Throwable {
    def exception = launchExpectingException(SliderClient,
        createTestConfig(),
        "org.apache.slider.core.exceptions.UsageException: Argument --name missing",
        [SliderActions.ACTION_REGISTRY])
    assert exception instanceof UsageException
    log.info(exception.toString())
  }

  @Test
  public void testRegistryExportBadUsage1() throws Throwable {
    def exception = launchExpectingException(SliderClient,
        createTestConfig(),
        "Expected a value after parameter --getexp",
        [SliderActions.ACTION_REGISTRY,
            Arguments.ARG_NAME,
            "cl1",
            Arguments.ARG_GETEXP])
    assert exception instanceof BadCommandArgumentsException
    log.info(exception.toString())
  }

  @Test
  public void testRegistryExportBadUsage2() throws Throwable {
    def exception = launchExpectingException(SliderClient,
        createTestConfig(),
        "Expected a value after parameter --getexp",
        [SliderActions.ACTION_REGISTRY,
            Arguments.ARG_NAME,
            "cl1",
            Arguments.ARG_LISTEXP,
        Arguments.ARG_GETEXP])
    assert exception instanceof BadCommandArgumentsException
    log.info(exception.toString())
  }

  @Test
  public void testRegistryExportBadUsage3() throws Throwable {
    def exception = launchExpectingException(SliderClient,
        createTestConfig(),
        "Usage: registry",
        [SliderActions.ACTION_REGISTRY,
            Arguments.ARG_NAME,
            "cl1",
            Arguments.ARG_LISTEXP,
            Arguments.ARG_GETEXP,
            "export1"])
    assert exception instanceof UsageException
    log.info(exception.toString())
  }

  @Test
  public void testUpgradeUsage() throws Throwable {
    def exception = launchExpectingException(SliderClient,
        createTestConfig(),
        "org.apache.slider.core.exceptions.BadCommandArgumentsException: Not enough arguments for action: upgrade Expected minimum 1 but got 0",
        [SliderActions.ACTION_UPGRADE])
    assert exception instanceof BadCommandArgumentsException
    log.info(exception.toString())
  }

  @Test
  public void testUpgradeWithTemplateOptionOnly() throws Throwable {
    String appName = "test_hbase"
    def exception = launchExpectingException(SliderClient,
        createTestConfig(),
        "BadCommandArgumentsException: Option --resources must be specified with option --template",
        [SliderActions.ACTION_UPGRADE,
            appName,
            Arguments.ARG_TEMPLATE,
            "/tmp/appConfig.json",
        ])
    assert exception instanceof BadCommandArgumentsException
    log.info(exception.toString())
  }

  public Configuration createTestConfig() {
    def configuration = new Configuration()
    configuration.set(YarnConfiguration.RM_ADDRESS,  "127.0.0.1:8032")
    return configuration
  }

  @Test
  public void testUpgradeWithResourcesOptionOnly() throws Throwable {
    String appName = "test_hbase"
    def exception = launchExpectingException(SliderClient,
        createTestConfig(),
        "BadCommandArgumentsException: Option --template must be specified with option --resources",
        [SliderActions.ACTION_UPGRADE,
            appName,
            Arguments.ARG_RESOURCES,
            "/tmp/resources.json",
        ])
    assert exception instanceof BadCommandArgumentsException
    log.info(exception.toString())
  }

  @Test
  public void testUpgradeWithTemplateResourcesAndContainersOption() throws Throwable {
    String appName = "test_hbase"
    def exception = launchExpectingException(SliderClient,
        createTestConfig(),
        "BadCommandArgumentsException: Option --containers cannot be "
        + "specified with --template or --resources",
        [SliderActions.ACTION_UPGRADE,
            appName,
            Arguments.ARG_TEMPLATE,
            "/tmp/appConfig.json",
            Arguments.ARG_RESOURCES,
            "/tmp/resources.json",
            Arguments.ARG_CONTAINERS,
            "container_1"
        ])
    assert exception instanceof BadCommandArgumentsException
    log.info(exception.toString())
  }

  @Test
  public void testUpgradeWithTemplateResourcesAndComponentsOption() throws Throwable {
    String appName = "test_hbase"
    def exception = launchExpectingException(SliderClient,
        createTestConfig(),
        "BadCommandArgumentsException: Option --components cannot be "
        + "specified with --template or --resources",
        [SliderActions.ACTION_UPGRADE,
            appName,
            Arguments.ARG_TEMPLATE,
            "/tmp/appConfig.json",
            Arguments.ARG_RESOURCES,
            "/tmp/resources.json",
            Arguments.ARG_COMPONENTS,
            "HBASE_MASTER"
        ])
    assert exception instanceof BadCommandArgumentsException
    log.info(exception.toString())
  }

  @Test
  public void testCreateAppWithAddonPkgBadArg1() throws Throwable {
    //add on package without specifying add on package name
      def exception = launchExpectingException(SliderClient,
          createTestConfig(),
          "Expected 2 values after --addon",
          [SliderActions.ACTION_CREATE,
              "cl1",
              Arguments.ARG_ADDON,
              "addon1"])
      assert exception instanceof BadCommandArgumentsException
      log.info(exception.toString())
    }

  @Test
  public void testNodesMissingFile() throws Throwable {
    def exception = launchExpectingException(SliderClient,
      createTestConfig(),
      "after parameter --out",
      [SliderActions.ACTION_NODES, Arguments.ARG_OUTPUT])
    assert exception instanceof BadCommandArgumentsException
  }

  @Test
  public void testFlexWithNoComponents() throws Throwable {
    def exception = launchExpectingException(SliderClient,
        new Configuration(),
        "Usage: slider flex <application>",
        [
          SliderActions.ACTION_FLEX,
          "flex1",
          Arguments.ARG_DEFINE, YarnConfiguration.RM_ADDRESS + "=127.0.0.1:8032"
        ])
    assert exception instanceof UsageException
    log.info(exception.toString())
  }
}
