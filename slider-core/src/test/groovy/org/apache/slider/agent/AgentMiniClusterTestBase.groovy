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

package org.apache.slider.agent

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream
import org.apache.commons.compress.utils.IOUtils
import org.apache.commons.io.FileUtils
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderXMLConfKeysForTesting
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.providers.agent.AgentKeys
import org.apache.slider.test.YarnZKMiniClusterTestBase
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.rules.TemporaryFolder
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * test base for agent clusters
 */
@CompileStatic
@Slf4j
public abstract class AgentMiniClusterTestBase
extends YarnZKMiniClusterTestBase {
  private static Logger LOG = LoggerFactory.getLogger(AgentMiniClusterTestBase)
  protected static File agentConf
  protected static File agentDef
  protected static Map<String, String> agentDefOptions
  private static TemporaryFolder tempFolder = new TemporaryFolder();

  /**
   * Server side test: validate system env before launch
   */
  @BeforeClass
  public static void checkSystem() {
//    SliderUtils.validateSliderServerEnvironment(LOG)
  }
  
  @BeforeClass
  public static void createSubConfFiles() {

    String s = File.separator
    File destDir = new File("target${s}agent_minicluster_testbase")
    destDir.mkdirs()
    agentConf = new File(destDir, "agentconf.zip")
    agentConf.createNewFile()
    agentDef = new File(destDir, "agentdef")
    agentDef.createNewFile()

    // dynamically create the app package for the test
    tempFolder.create()
    def pkgPath = tempFolder.newFolder("testpkg")
    File imagePath = new File(pkgPath, "appdef_1.zip").canonicalFile
    File metainfo = new File(new File(".").absoluteFile, "src${s}test${s}python${s}metainfo.xml");
    ZipArchiveOutputStream zipFile = new ZipArchiveOutputStream(new FileOutputStream(imagePath));
    try {
      zipFile.putArchiveEntry(new ZipArchiveEntry(metainfo.name));
      IOUtils.copy(new FileInputStream(metainfo), zipFile);
      zipFile.closeArchiveEntry();
    }
    finally {
      zipFile.close();
    }

    agentDefOptions = [
        (AgentKeys.APP_DEF): imagePath.toURI().toString(),
        (AgentKeys.AGENT_CONF): agentConf.toURI().toString()
    ]
  }

  @AfterClass
  public static void cleanSubConfFiles() {
    def tempRoot
    try {
      tempRoot = tempFolder.root
      if (tempRoot.exists()) {
        FileUtils.deleteDirectory(tempRoot);
      }
    } catch (IOException e) {
      log.info("Failed to delete $tempRoot :$e", e)
    } catch (IllegalStateException e) {
      log.warn("Temp folder deletion failed: $e")
    }
  }

  public static String createAddOnPackageFiles() {
    String s = File.separator

    File destDir = new File("target${s}agent_minicluster_testbase_addon")
    destDir.mkdirs()
    File addonAgentConf = new File(destDir, "addon1.zip")
    addonAgentConf.createNewFile()

    // dynamically create the app package for the test
    TemporaryFolder addonTempFolder = new TemporaryFolder();
    addonTempFolder.create()
    def pkgPath = addonTempFolder.newFolder("testpkg")
    File imagePath = new File(pkgPath, "appdef_1.zip").canonicalFile
    File metainfo = new File(new File(".").absoluteFile,
      "src${s}test${s}python${s}metainfo.xml");
    ZipArchiveOutputStream zipFile = new ZipArchiveOutputStream(
      new FileOutputStream(imagePath));
    try {
      zipFile.putArchiveEntry(new ZipArchiveEntry(metainfo.name));
      IOUtils.copy(new FileInputStream(metainfo), zipFile);
      zipFile.closeArchiveEntry();
    }
    finally {
      zipFile.close();
    }
    return addonAgentConf.toURI().toString()
  }

  @Override
  public String getTestConfigurationPath() {
    return "src/main/resources/" + AgentKeys.CONF_RESOURCE;
  }

  @Override
  void setup() {
    super.setup()
    def testConf = getTestConfiguration()
  }

  /**
   * Teardown kills region servers
   */
  @Override
  void teardown() {
    super.teardown();
    if (teardownKillall) {
    }
  }

  @Override
  String getApplicationHomeKey() {
    return SliderXMLConfKeysForTesting.KEY_TEST_AGENT_HOME;
  }

  @Override
  String getArchiveKey() {
    return SliderXMLConfKeysForTesting.KEY_TEST_AGENT_TAR;
  }

  /**
   return a mock home dir -this test case does not intend to run any agent
   */
  @Override
  List<String> getImageCommands() {
    [Arguments.ARG_IMAGE, agentDef.toURI().toString()]
  }

/**
 * Create a standalone AM
 * @param clustername AM name
 * @param size # of nodes
 * @param deleteExistingData should any existing cluster data be deleted
 * @param blockUntilRunning block until the AM is running
 * @return launcher which will have executed the command.
 */
  public ServiceLauncher<SliderClient> createStandaloneAM(
      String clustername,
      boolean deleteExistingData,
      boolean blockUntilRunning) {
    List<String> args = [];
    return createStandaloneAMWithArgs(
        clustername,
        args,
        deleteExistingData,
        blockUntilRunning)
  }

/**
 * Create an AM without a master
 * @param clustername AM name
 * @param extraArgs extra arguments
 * @param size # of nodes
 * @param deleteExistingData should any existing cluster data be deleted
 * @param blockUntilRunning block until the AM is running
 * @return launcher which will have executed the command.
 */
  public ServiceLauncher<SliderClient> createStandaloneAMWithArgs(
      String clustername,
      List<String> extraArgs,
      boolean deleteExistingData,
      boolean blockUntilRunning) {
    if (hdfsCluster) {
      fail("Agent tests do not (currently) work with mini HDFS cluster")
    }
    return createCluster(clustername,
        [:],
        extraArgs,
        deleteExistingData,
        blockUntilRunning,
        agentDefOptions)
  }

}