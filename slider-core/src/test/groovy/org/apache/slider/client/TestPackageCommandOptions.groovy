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

import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RawLocalFileSystem
import org.apache.hadoop.yarn.conf.YarnConfiguration

import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.ClientArgs
import org.apache.slider.common.params.SliderActions
import org.apache.slider.common.tools.SliderFileSystem
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.providers.agent.AgentKeys

import org.junit.Before
import org.junit.Test

/**
 * Test the package commands options
 */
class TestPackageCommandOptions extends AgentMiniClusterTestBase {
  final shouldFail = new GroovyTestCase().&shouldFail
  private static SliderFileSystem testFileSystem
  private static String APP_NAME = "HBASE"
  private static String APP_VERSION = "1.0.0"
  
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
  public void testPackageInstall() throws Throwable {
    // create a mock app package file
    File localPackage =
        FileUtil.createLocalTempFile(tempLocation, "hbase.zip", false)
    String contents = UUID.randomUUID().toString()
    FileUtils.write(localPackage, contents)
    // install the package
    YarnConfiguration conf = SliderUtils.createConfiguration()
    ServiceLauncher launcher = launch(TestSliderClient,
        conf,
        [
          ClientArgs.ACTION_PACKAGE,
          ClientArgs.ARG_INSTALL,
          ClientArgs.ARG_NAME,
          APP_NAME,
          ClientArgs.ARG_PACKAGE,
          localPackage.absolutePath
        ])
    Path installedPath = new Path(testFileSystem.buildPackageDirPath(APP_NAME,
      null), localPackage.getName())
    File installedPackage = new File(installedPath.toUri().path)
    // verify file was installed successfully
    assert installedPackage.exists()
    assert FileUtils.readFileToString(installedPackage).equals(
      FileUtils.readFileToString(localPackage))
  }

  @Test
  public void testPackageInstances() throws Throwable {
    describe("Create mini cluster")
    YarnConfiguration yarnConfig = new YarnConfiguration(configuration)
    String clustername = createMiniCluster("", yarnConfig, 1, true)
    describe("Created cluster - " + clustername)

    // get the default application.def file and install it as a package
    String appDefPath = agentDefOptions.getAt(AgentKeys.APP_DEF)
    File appDefFile = new File(new URI(appDefPath).path)
    assert appDefFile.exists()
    YarnConfiguration conf = SliderUtils.createConfiguration()
    ServiceLauncher<SliderClient> launcher = launch(TestSliderClient,
        conf,
        [
          ClientArgs.ACTION_PACKAGE,
          ClientArgs.ARG_INSTALL,
          ClientArgs.ARG_NAME,
          APP_NAME,
          ClientArgs.ARG_PACKAGE,
          appDefFile.absolutePath
        ])
    Path installedPath = new Path(testFileSystem.buildPackageDirPath(APP_NAME,
      null), appDefFile.getName())
    File installedPackage = new File(installedPath.toUri().path)
    assert installedPackage.exists()

    def installedPackageURI = installedPackage.toURI()
    describe("Installed app package to - $installedPackageURI")
    // overwrite the application.def property with the new installed path
    agentDefOptions.put(AgentKeys.APP_DEF, installedPackageURI.toString())
    // start the app and AM
    describe("Starting the app")
    launcher = createStandaloneAM(clustername, true, false)
    SliderClient sliderClient = launcher.service
    addToTeardown(sliderClient)

    describe("Listing all instances of installed packages")
    String outFileName = "target${File.separator}packageInstances.out"
    File outFile = new File(outFileName)

    launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        yarnConfig,
        //varargs list of command line params
        [SliderActions.ACTION_PACKAGE,
         Arguments.ARG_PKGINSTANCES,
         ClientArgs.ARG_OUTPUT,
         outFile.absolutePath
        ]
    )
    // reset the app def path to orig value and remove app version
    agentDefOptions.put(AgentKeys.APP_DEF, appDefPath)
    agentDefOptions.remove(SliderKeys.APP_VERSION)

    assert launcher.serviceExitCode == 0
    def client = launcher.service
    def instances = client.enumSliderInstances(false, null, null)
    def enumeratedInstance = instances[clustername]
    assert enumeratedInstance != null
    assert enumeratedInstance.applicationReport.name ==
           clustername
    def outText = outFile.text
    assert outText.contains(installedPackage.toURI().toString())
    assert outText.contains(APP_NAME)
    assert outText.contains(clustername)
    assert outText.matches("(?s).*" + clustername + " +" + APP_NAME + " +"
      + installedPackage.toURI() + ".*")
  }

  @Test
  public void testPackageInstancesWithVersion() throws Throwable {
    describe("Create mini cluster")
    YarnConfiguration yarnConfig = new YarnConfiguration(configuration)
    String clustername = createMiniCluster("", yarnConfig, 1, true)
    describe("Created cluster - " + clustername)

    // get the default application.def file and install it as a package
    String appDefPath = agentDefOptions.getAt(AgentKeys.APP_DEF)
    File appDefFile = new File(new URI(appDefPath).path)
    YarnConfiguration conf = SliderUtils.createConfiguration()
    ServiceLauncher<SliderClient> launcher = launch(TestSliderClient,
        conf,
        [
          ClientArgs.ACTION_PACKAGE,
          ClientArgs.ARG_INSTALL,
          ClientArgs.ARG_NAME,
          APP_NAME,
          ClientArgs.ARG_PACKAGE,
          appDefFile.absolutePath,
          ClientArgs.ARG_VERSION,
          APP_VERSION
        ])
    Path installedPath = new Path(testFileSystem.buildPackageDirPath(APP_NAME,
      APP_VERSION), appDefFile.getName())
    File installedPackage = new File(installedPath.toUri().path)
    assert installedPackage.exists()
    describe("Installed app package to - " + installedPackage.toURI()
      .toString())
    // overwrite the application.def property with the new installed path
    agentDefOptions.put(AgentKeys.APP_DEF, installedPackage.toURI()
      .toString())
    // add the app version
    agentDefOptions.put(SliderKeys.APP_VERSION, APP_VERSION)
    // start the app and AM
    describe("Starting the app")
    launcher = createStandaloneAM(clustername, true, false)
    SliderClient sliderClient = launcher.service
    addToTeardown(sliderClient)

    describe("Listing all instances of installed packages")
    File outFile = new File("target${File.separator}packageInstancesWithVersion.out")
    launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        yarnConfig,
        //varargs list of command line params
        [
          SliderActions.ACTION_PACKAGE,
          Arguments.ARG_PKGINSTANCES,
          ClientArgs.ARG_OUTPUT,
          outFile.absolutePath
        ]
    )
    // reset the app def path to orig value and remove app version
    agentDefOptions.put(AgentKeys.APP_DEF, appDefPath)
    agentDefOptions.remove(SliderKeys.APP_VERSION)

    assert launcher.serviceExitCode == 0
    def client = launcher.service
    def instances = client.enumSliderInstances(false, null, null)
    def enumeratedInstance = instances[clustername]
    assert enumeratedInstance != null
    assert enumeratedInstance.applicationReport.name == clustername
    def outText = outFile.text
    assert outText.contains(installedPackage.toURI().toString())
    assert outText.contains(APP_NAME)
    assert outText.contains(clustername)
    assert outText.matches("(?s).*" + clustername + " +" + APP_NAME + " +"
      + APP_VERSION + " +" + installedPackage.toURI() + ".*")
  }

  private File getTempLocation () {
    File tmpDir = File.createTempFile("temp", "dir")
    tmpDir.delete()
    tmpDir.mkdir()
    tmpDir
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
