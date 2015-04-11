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

import java.io.File
import java.io.IOException
import java.io.FileNotFoundException

import org.apache.commons.io.FileUtils
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RawLocalFileSystem
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.ClientArgs
import org.apache.slider.common.params.SliderActions
import org.apache.slider.common.tools.SliderFileSystem
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.exceptions.BadCommandArgumentsException
import org.apache.slider.core.exceptions.BadConfigException
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.providers.agent.AgentKeys

import org.junit.Before
import org.junit.Test

/**
 * Test the package commands options
 */
class TestUpgradeCommandOptions extends AgentMiniClusterTestBase {
  final shouldFail = new GroovyTestCase().&shouldFail
  private Log log = LogFactory.getLog(this.class)
  private static SliderFileSystem testFileSystem
  private static String APP_NAME = "HBASE"
  private static String APP_VERSION = "1.0.0"
  private YarnConfiguration yarnConfig = new YarnConfiguration(configuration)
  private ServiceLauncher<SliderClient> launcher = null
  
  @Before
  public void setupFilesystem() {
    FileSystem fileSystem = new RawLocalFileSystem()
    YarnConfiguration configuration = SliderUtils.createConfiguration()
    fileSystem.setConf(configuration)
    testFileSystem = new SliderFileSystem(fileSystem, configuration)
    File testFolderDir = new File(testFileSystem.buildPackageDirPath(APP_NAME,
      null).toUri().path)
    testFolderDir.deleteDir()
    File testFolderDirWithVersion = new File(testFileSystem.buildPackageDirPath(
      APP_NAME, APP_VERSION).toUri().path)
    testFolderDirWithVersion.deleteDir()
  }

  @Test
  public void testUpgradeAppNotRunning() throws Throwable {
    describe("Calling upgrade")
    YarnConfiguration conf = SliderUtils.createConfiguration()
    try {
      ServiceLauncher launcher = launch(TestSliderClient,
          conf,
          [
            ClientArgs.ACTION_UPGRADE,
            APP_NAME,
            ClientArgs.ARG_TEMPLATE,
            "/tmp/appConfig.json",
            ClientArgs.ARG_RESOURCES,
            "/tmp/resources.json"
          ])
      fail("Upgrade command should have failed")
    } catch (SliderException e) {
      assert e instanceof UnknownApplicationInstanceException
      assert e.getMessage().contains("Unknown application instance")
      log.info(e.toString())
    }
  }

  @Test
  public void testAll() {
    // Create a single test to reduce the amount of test execution time
    describe("Create mini cluster")
    String clustername = createMiniCluster("", yarnConfig, 1, true)
    describe("Created cluster - " + clustername)

    // start the app and AM
    describe("Starting the app")
    launcher = createStandaloneAM(clustername, true, false)
    SliderClient sliderClient = launcher.service
    ApplicationReport report = waitForClusterLive(sliderClient)
    addToTeardown(sliderClient)

    // now call all the tests
    testUpgradeInvalidResourcesFile(clustername)
    testUpgradeInvalidConfigFile(clustername)
    testUpgradeSuccess(clustername)
  }

  public void testUpgradeInvalidResourcesFile(String clustername) 
    throws Throwable {
    String appConfigFile = Path.getPathWithoutSchemeAndAuthority(testFileSystem
      .buildClusterDirPath(clustername)).toString() + "/app_config.json"

    describe("Calling upgrade - testUpgradeInvalidResourcesFile")
    try {
      launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          yarnConfig,
          //varargs list of command line params
          [
              ClientArgs.ACTION_UPGRADE,
              clustername,
              ClientArgs.ARG_TEMPLATE,
              appConfigFile,
              ClientArgs.ARG_RESOURCES,
              "/tmp/resources.json"
          ]
      )
      fail("Upgrade command should have failed")
    } catch (SliderException e) {
      assert e instanceof BadConfigException
      assert e.getMessage().contains("incorrect argument to --resources: " +
        "\"/tmp/resources.json\" : java.io.FileNotFoundException: " +
        "/tmp/resources.json (No such file or directory)")
      log.info(e.toString())
    }
  }

  public void testUpgradeInvalidConfigFile(String clustername)
    throws Throwable {
    String resourceFile = Path.getPathWithoutSchemeAndAuthority(testFileSystem
      .buildClusterDirPath(clustername)).toString() + "/resources.json"

    describe("Calling upgrade - testUpgradeInvalidConfigFile")
    try {
      launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          yarnConfig,
          //varargs list of command line params
          [
              ClientArgs.ACTION_UPGRADE,
              clustername,
              ClientArgs.ARG_TEMPLATE,
              "/tmp/appConfig.json",
              ClientArgs.ARG_RESOURCES,
              resourceFile
          ]
      )
      fail("Upgrade command should have failed")
    } catch (SliderException e) {
      assert e instanceof BadConfigException
      assert e.getMessage().contains("incorrect argument to --template: " +
        "\"/tmp/appConfig.json\" : java.io.FileNotFoundException: " +
        "/tmp/appConfig.json (No such file or directory)")
      log.info(e.toString())
    }
  }

  public void testUpgradeSuccess(String clustername)
    throws Throwable {
    String resourceFile = Path.getPathWithoutSchemeAndAuthority(testFileSystem
      .buildClusterDirPath(clustername)).toString() + "/resources.json"
    String appConfigFile = Path.getPathWithoutSchemeAndAuthority(testFileSystem
      .buildClusterDirPath(clustername)).toString() + "/app_config.json"

    describe("Calling upgrade - testUpgradeSuccess")
    try {
      launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          yarnConfig,
          //varargs list of command line params
          [
              ClientArgs.ACTION_UPGRADE,
              clustername,
              ClientArgs.ARG_TEMPLATE,
              appConfigFile,
              ClientArgs.ARG_RESOURCES,
              resourceFile
          ]
      )
    } catch (SliderException e) {
      fail("Upgrade command should have failed")
      log.info(e.toString())
    }
    assert launcher.serviceExitCode == 0
  }

  private File getTempLocation () {
    return new File(System.getProperty("user.dir") + "/target/_")
  }

  static class TestSliderClient extends SliderClient {
    public TestSliderClient() {
      super()
    }

    @Override
    protected void initHadoopBinding() throws IOException, SliderException {
      sliderFileSystem = testFileSystem
    }
  }
}
