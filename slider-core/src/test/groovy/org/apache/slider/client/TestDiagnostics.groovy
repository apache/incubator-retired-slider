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

package org.apache.slider.client

import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.RawLocalFileSystem
import org.apache.hadoop.security.KerberosDiags
import org.apache.hadoop.yarn.api.records.ApplicationReport
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.yarn.conf.YarnConfiguration
import static org.apache.slider.common.Constants.SUN_SECURITY_KRB5_DEBUG
import org.apache.slider.agent.AgentMiniClusterTestBase
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.params.ActionDiagnosticArgs
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.ClientArgs
import org.apache.slider.common.params.SliderActions
import org.apache.slider.common.tools.SliderFileSystem
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.exceptions.SliderException
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.providers.agent.AgentKeys
import org.apache.slider.test.YarnZKMiniClusterTestBase
import org.junit.Test

@Slf4j
class TestDiagnostics extends AgentMiniClusterTestBase {
  private static SliderFileSystem testFileSystem
  private static String APP_NAME = "HBASE"
  private static String APP_VERSION = "1.0.0"

  @Test
  public void testClientDiags() throws Throwable {
    //launch fake master
    String clustername = createMiniCluster("", configuration, 1, true)
    ServiceLauncher<SliderClient> launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [SliderActions.ACTION_DIAGNOSTICS,
         Arguments.ARG_CLIENT]
    )
    def client = launcher.service
    def diagnostics = new ActionDiagnosticArgs()
    diagnostics.client = true
    diagnostics.verbose = true
    describe("Verbose diagnostics")
    client.actionDiagnostic(diagnostics)
  }

  /**
   * help should print out help string and then succeed
   * @throws Throwable
   */
  @Test
  public void testKDiag() throws Throwable {
    ServiceLauncher launcher = launch(SliderClient,
      SliderUtils.createConfiguration(),
      [
        ClientArgs.ACTION_KDIAG,
        ClientArgs.ARG_KEYLEN, "128",
        ClientArgs.ARG_SYSPROP,
        define(SUN_SECURITY_KRB5_DEBUG, "true")])

    assert 0 == launcher.serviceExitCode
  }

  @Test
  public void testKDiagExceptionConstruction() throws Throwable {
    assert new KerberosDiags.KerberosDiagsFailure("CAT", "%02d", 3).toString().contains("03")
    assert new KerberosDiags.KerberosDiagsFailure("CAT", "%w").toString().contains("%w")
    assert new KerberosDiags.KerberosDiagsFailure("CAT", new Exception(), "%w")
      .toString().contains("%w")
  }

  @Test
  public void testKDiagPrintln() throws Throwable {
    assert "%w" == KerberosDiags.format("%w")
    assert "%s" == KerberosDiags.format("%s")
    assert "false" == KerberosDiags.format("%s", false)
    def sw = new StringWriter()
    def kdiag = new KerberosDiags(new Configuration(),
      new PrintWriter(sw), [], null, "self", 16, false)
    try {
      kdiag.println("%02d", 3)
      kdiag.println("%s")
      kdiag.println("%w")
    } finally {
      kdiag.close()
    }
    def output = sw.toString()
    assert output.contains("03")
    assert output.contains("%s")
    assert output.contains("%w")
  }

  @Test
  public void testKDiagDumpFile() throws Throwable {
    def file1 = new File("./target/kdiaginput.txt")

    def s = 'invalid %w string %s'
    file1 << s
    def sw = new StringWriter()
    def kdiag = new KerberosDiags(new Configuration(),
      new PrintWriter(sw), [], null, "self", 16, false)
    try {
      kdiag.dump(file1)
    } finally {
      kdiag.close()
    }
    def output = sw.toString()
    assert output.contains(s)
  }

  @Test
  public void testContainerDiagsNoAppContainer() throws Throwable {
    super.setup()
    describe("Create a live cluster then run the container diagnostics command")
    createMiniCluster("testactiondiag", configuration, 1, true)
    String clustername = "testdiagclusternoappcontainers"
    
    //launch the cluster
	describe("Run an app with AM only and no app containers")
    ServiceLauncher<SliderClient> launcher = createStandaloneAM(
        clustername,
        true,
        false)

    SliderClient sliderClient = launcher.service
    ApplicationReport report = waitForClusterLive(sliderClient)

    //now look for the explicit service
	describe("Now running diagnostics command")
    ActionDiagnosticArgs diagArgs = new ActionDiagnosticArgs()
    diagArgs.name = clustername
    diagArgs.containers = true
    int status = sliderClient.actionDiagnostic(diagArgs)
    assert 0 == status

    //now exec the status command
    ServiceLauncher diagLauncher = launchClientAgainstMiniMR(
        //config includes RM binding info
        new YarnConfiguration(miniCluster.config),
        //varargs list of command line params
        [
            SliderActions.ACTION_DIAGNOSTICS,
            Arguments.ARG_NAME,
            clustername,
            Arguments.ARG_CONTAINERS,
            Arguments.ARG_MANAGER, RMAddr,
        ]
        
    )
    assert diagLauncher.serviceExitCode == 0

  }

  @Test
  public void testContainerDiagsWithAppPackage() throws Throwable {
    super.setup()
    FileSystem fileSystem = new RawLocalFileSystem()
    YarnConfiguration configuration = SliderUtils.createConfiguration()
    fileSystem.setConf(configuration)
    testFileSystem = new SliderFileSystem(fileSystem, configuration)
    describe("Create a live cluster then run the container diagnostics command")
    createMiniCluster("testactiondiag", configuration, 1, true)

    YarnConfiguration yarnConfig = new YarnConfiguration(configuration)
    String clustername = "testdiagclusterwithappcontainers"
    // get the default application.def file and install it as a package
    String appDefPath = agentDefOptions.getAt(AgentKeys.APP_DEF)
    File appDefFile = new File(new URI(appDefPath))
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
          APP_VERSION,
          ClientArgs.ARG_REPLACE_PKG
        ])
    Path installedPath = new Path(testFileSystem.buildPackageDirPath(APP_NAME,
      APP_VERSION), appDefFile.getName())
    File installedPackage = new File(installedPath.toUri().path)
    assert installedPackage.exists()
    describe("Installed app package to - " + installedPackage.toURI()
      .toString())
    // overwrite the application.def property with the new installed path
    agentDefOptions.putAt(AgentKeys.APP_DEF, installedPackage.toURI()
      .toString())
    // add the app version
    agentDefOptions.putAt(SliderKeys.APP_VERSION, APP_VERSION)
    // start the app and AM
    describe("Starting the app")
    launcher = createStandaloneAM(clustername, true, false)
    SliderClient sliderClient = launcher.service
    addToTeardown(sliderClient)
    waitForClusterLive(sliderClient)

	describe("Now running diagnostics command")
    launcher = launchClientAgainstMiniMR(
        //config includes RM binding info
        yarnConfig,
        //varargs list of command line params
        [SliderActions.ACTION_DIAGNOSTICS,
         Arguments.ARG_NAME,
         clustername,
         Arguments.ARG_CONTAINERS
        ]
    )

    assert launcher.serviceExitCode == 0
    def client = launcher.service
    def instances = client.enumSliderInstances(false, null, null)
    assert instances.size() > 0
    def enumeratedInstance = instances[clustername]
    assert enumeratedInstance != null
    assert enumeratedInstance.applicationReport != null
    assert enumeratedInstance.applicationReport.name ==
           clustername
    assert enumeratedInstance.name == clustername
    assert enumeratedInstance.path.toString().endsWith("/" + clustername)
    assert enumeratedInstance.applicationReport.yarnApplicationState == 
      YarnApplicationState.RUNNING
    instances = sliderClient.enumSliderInstances(true,
      YarnApplicationState.RUNNING, YarnApplicationState.RUNNING)
    assert instances[clustername]

    clusterActionFreeze(sliderClient, clustername, "stopping the cluster")
    waitForAppToFinish(sliderClient)
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
