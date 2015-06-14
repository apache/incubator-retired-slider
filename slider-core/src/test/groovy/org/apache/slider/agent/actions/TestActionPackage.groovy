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

package org.apache.slider.agent.actions

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.core.exceptions.BadCommandArgumentsException
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Before
import org.junit.Ignore
import org.junit.Test

/**
 * Test case for Action package
 */
@CompileStatic
@Slf4j

class TestActionPackage extends AgentMiniClusterTestBase {


  public static final String E_INVALID_APP_TYPE =
      "A valid application type name is required (e.g. HBASE)"

  String s = File.separator
  File packageFile = new File("src${s}test${s}resources${s}log4j.properties")

  @Before
  public void setup() {
    super.setup()
    createMiniCluster("", configuration, 1, false)
    assert packageFile.exists()
  }

  @Test
  public void testPackageInstallFailsWithNoPackageName() throws Throwable {
    try {
      ServiceLauncher launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_INSTALL
          ],
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (BadCommandArgumentsException e) {
      assertExceptionDetails(e,
          LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR,
          E_INVALID_APP_TYPE)
    }
  }

  @Test
  public void testPackageInstallFailsWithNoPackagePath() throws Throwable {
    try {
      ServiceLauncher launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_INSTALL,
              Arguments.ARG_NAME, "hbase"
          ],
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (BadCommandArgumentsException e) {
      assertExceptionDetails(e, LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR,
          SliderClient.E_INVALID_APPLICATION_PACKAGE_LOCATION);
    }
  }

  @Test
  public void testPackageInstallFailsOverwriteRequired() throws Throwable {
    try {
      ServiceLauncher launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_INSTALL,
              Arguments.ARG_NAME, "hbase",
              Arguments.ARG_PACKAGE, packageFile.absolutePath
          ],
      )
      launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_INSTALL,
              Arguments.ARG_NAME, "hbase",
              Arguments.ARG_PACKAGE, packageFile.absolutePath
          ],
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (BadCommandArgumentsException e) {
      assertExceptionDetails(e,
          LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR,
          SliderClient.E_USE_REPLACEPKG_TO_OVERWRITE)
    }
  }

  @Test
  public void testPackageInstallFailsUnableToReadPackageFile() throws Throwable {
    try {
      ServiceLauncher launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_INSTALL,
              Arguments.ARG_NAME, "hbase",
              Arguments.ARG_PACKAGE, "unlikely_to_be_a_file_path",
          ],
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (BadCommandArgumentsException e) {
      assertExceptionDetails(e,
          LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR,
          SliderClient.E_UNABLE_TO_READ_SUPPLIED_PACKAGE_FILE)
    }
  }

  @Test
  @Ignore
  public void testPackageInstallWithReplace() throws Throwable {
    try {
      ServiceLauncher launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_INSTALL,
              Arguments.ARG_NAME, "hbase",
              Arguments.ARG_PACKAGE, packageFile.absolutePath
          ],
        )
        launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_INSTALL,
              Arguments.ARG_NAME, "hbase",
              Arguments.ARG_PACKAGE, packageFile.absolutePath,
              Arguments.ARG_REPLACE_PKG
          ],
        )
    } catch (BadCommandArgumentsException e) {
      log.info(e.message)
      // throw e;
    }
  }

  @Test
  @Ignore
  public void testPackageList() throws Throwable {
    try {
      ServiceLauncher launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_INSTALL,
              Arguments.ARG_PKGLIST
          ],
      )
    } catch (BadCommandArgumentsException e) {
      log.info(e.message)
     // throw e;
    }
  }

  @Test
  @Ignore
  public void testPackageInstances() throws Throwable {
    try {
      ServiceLauncher launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_INSTALL,
              Arguments.ARG_PKGINSTANCES
          ],
      )
    } catch (BadCommandArgumentsException e) {
      log.info(e.message)
      // throw e;
    }
  }

  @Test
  public void testPackageDelete() throws Throwable {
      def uuid = UUID.randomUUID()
      ServiceLauncher launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_INSTALL,
              Arguments.ARG_NAME, "storm_" + uuid,
              Arguments.ARG_PACKAGE, packageFile.absolutePath,
          ],
      )
      launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_PKGDELETE,
              Arguments.ARG_NAME, "storm_" + uuid
          ],
      )
  }

  @Test
  public void testPackageDeleteFail() throws Throwable {
    try {
      ServiceLauncher launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_PKGDELETE,
              Arguments.ARG_NAME, "hbase1"
          ],
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (BadCommandArgumentsException e) {
      assertExceptionDetails(e,
          LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR,
          SliderClient.E_PACKAGE_DOES_NOT_EXIST)
    }
  }

  @Test
  public void testPackageDeleteFailsWithNoPackageName() throws Throwable {
    try {
      ServiceLauncher launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          new YarnConfiguration(miniCluster.config),
          //varargs list of command line params
          [
              SliderActions.ACTION_PACKAGE,
              Arguments.ARG_PKGDELETE
          ],
      )
      fail("expected an exception, got a status code " + launcher.serviceExitCode)
    } catch (BadCommandArgumentsException e) {
      assertExceptionDetails(e,
          LauncherExitCodes.EXIT_COMMAND_ARGUMENT_ERROR,
          E_INVALID_APP_TYPE)
    }
  }
}
