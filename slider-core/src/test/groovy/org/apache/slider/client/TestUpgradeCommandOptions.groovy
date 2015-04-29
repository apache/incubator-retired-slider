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

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RawLocalFileSystem
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.ClientArgs
import org.apache.slider.common.tools.SliderFileSystem
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.exceptions.BadConfigException
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException
import org.apache.slider.core.main.LauncherExitCodes
import org.apache.slider.core.main.ServiceLauncher
import org.junit.Before
import org.junit.Test

/**
 * Test the upgrade commands options
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
    File tmpDir = createTempDir()
    try {
      ServiceLauncher launcher = launch(TestSliderClient,
          conf,
          [
              ClientArgs.ACTION_UPGRADE,
              APP_NAME,
              ClientArgs.ARG_TEMPLATE,
              new File(tmpDir, "appConfig.json").toURI(),
              ClientArgs.ARG_RESOURCES,
              new File(tmpDir, "resources.json").toURI()
          ])
      fail("Upgrade command should have failed")
    } catch (SliderException e) {
      log.info(e.toString())
      assert e instanceof UnknownApplicationInstanceException
      assert e.getMessage().contains("Unknown application instance")
    }
  }

  public File createTempDir() {
    File tmpDir = File.createTempFile("test", ".dir")
    tmpDir.delete()
    return tmpDir
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

    // Now call all the tests.
    // These tests are written in a way where one depends on the other (like
    // testUpgradeInvalidContainers depends on testUpgradeSpecSuccess). So it
    // is important to run them all together and in the listed order.
    testUpgradeInvalidResourcesFile(clustername)
    testUpgradeInvalidConfigFile(clustername)
    testUpgradeSpecSuccess(clustername)
    testUpgradeInvalidContainers(clustername)
    testUpgradeInvalidComponents(clustername)
    testUpgradeInvalidContainersAndComponents(clustername)
  }

  public void testUpgradeInvalidResourcesFile(String clustername) 
    throws Throwable {
    Path appConfigFile = getClusterFile(clustername, "app_config.json")
    File tmpDir = createTempDir()
    File resourcesJson = new File(tmpDir, "resources.json")

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
              resourcesJson.absolutePath
          ]
      )
      fail("Upgrade command should have failed")
    } catch (BadConfigException e) {
      assertExceptionDetails(e, SliderExitCodes.EXIT_BAD_CONFIGURATION,
          Arguments.ARG_RESOURCES)
    }
  }

  public Path getClusterFile(String clustername, String clusterfile) {
    Path path = new Path(testFileSystem.buildClusterDirPath(clustername), clusterfile)
    return Path.getPathWithoutSchemeAndAuthority(path)
  }

  public void testUpgradeInvalidConfigFile(String clustername)
    throws Throwable {
    Path resourceFile = getClusterFile(clustername, "resources.json")
    File tmpDir = createTempDir()
    File appconfig = new File(tmpDir, "appConfig.json")


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
              appconfig.absolutePath,
              ClientArgs.ARG_RESOURCES,
              resourceFile
          ]
      )
      fail("Upgrade command should have failed")
    } catch (BadConfigException e) {
      assertExceptionDetails(e, SliderExitCodes.EXIT_BAD_CONFIGURATION,
          Arguments.ARG_TEMPLATE)
    }
  }

  public void testUpgradeSpecSuccess(String clustername)
    throws Throwable {
    String resourceFile = Path.getPathWithoutSchemeAndAuthority(testFileSystem
      .buildClusterDirPath(clustername)).toString() + "/resources.json"
    String appConfigFile = Path.getPathWithoutSchemeAndAuthority(testFileSystem
      .buildClusterDirPath(clustername)).toString() + "/app_config.json"

    describe("Calling upgrade - testUpgradeSpecSuccess")
    log.info("Listing application containers before upgrade spec")
    launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        yarnConfig,
        //varargs list of command line params
        [
            ClientArgs.ACTION_LIST,
            clustername,
            ClientArgs.ARG_CONTAINERS
        ]
    )

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

    log.info("Listing application containers after upgrade spec")
    launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        yarnConfig,
        //varargs list of command line params
        [
            ClientArgs.ACTION_LIST,
            clustername,
            ClientArgs.ARG_CONTAINERS
        ]
    )
    assert launcher.serviceExitCode == 0
  }

  public void testUpgradeInvalidContainers(String clustername)
    throws Throwable {
    describe("Calling upgrade - testUpgradeInvalidContainers")
    try {
      launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          yarnConfig,
          //varargs list of command line params
          [
              ClientArgs.ACTION_UPGRADE,
              clustername,
              ClientArgs.ARG_CONTAINERS,
              "container_1_invalid"
          ]
      )
      fail("Upgrade command should have failed")
    } catch (SliderException e) {
      log.info(e.toString())
    }
    assert launcher.serviceExitCode == 0
  }

  public void testUpgradeInvalidComponents(String clustername)
    throws Throwable {
    describe("Calling upgrade - testUpgradeInvalidComponents")
    try {
      launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          yarnConfig,
          //varargs list of command line params
          [
              ClientArgs.ACTION_UPGRADE,
              clustername,
              ClientArgs.ARG_COMPONENTS,
              "HBASE_ROLE_INVALID"
          ]
      )
      fail("Upgrade command should have failed")
    } catch (SliderException e) {
      log.info(e.toString())
    }
    assert launcher.serviceExitCode == 0
  }

  public void testUpgradeInvalidContainersAndComponents(String clustername)
    throws Throwable {
    describe("Calling upgrade - testUpgradeInvalidContainersAndComponents")
    try {
      launcher = launchClientAgainstMiniMR(
          //config includes RM binding info
          yarnConfig,
          //varargs list of command line params
          [
              ClientArgs.ACTION_UPGRADE,
              clustername,
              ClientArgs.ARG_CONTAINERS,
              "container_1_invalid",
              "container_2_invalid",
              ClientArgs.ARG_COMPONENTS,
              "HBASE_MASTER_ROLE_INVALID",
              "HBASE_RS_ROLE_INVALID"
          ]
      )
      fail("Upgrade command should have failed")
    } catch (SliderException e) {
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
