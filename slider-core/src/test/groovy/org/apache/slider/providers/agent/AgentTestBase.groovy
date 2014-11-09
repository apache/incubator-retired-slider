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

package org.apache.slider.providers.agent

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream
import org.apache.commons.compress.utils.IOUtils
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.client.SliderClient
import org.apache.slider.common.params.SliderActions
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.main.ServiceLauncher
import org.apache.slider.test.YarnZKMiniClusterTestBase
import org.junit.Before
import org.junit.Rule
import org.junit.rules.TemporaryFolder

import static org.apache.slider.common.SliderXMLConfKeysForTesting.*
import static org.apache.slider.providers.agent.AgentKeys.CONF_RESOURCE

/**
 * test base for all agent clusters
 */
@CompileStatic
@Slf4j
public abstract class AgentTestBase extends YarnZKMiniClusterTestBase {

  public static final int AGENT_CLUSTER_STARTUP_TIME = 1000 * DEFAULT_AGENT_LAUNCH_TIME_SECONDS

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  /**
   * Server side test: validate system env before launch
   */
  public static void assumeValidServerEnv() {
    try {
      SliderUtils.validateSliderServerEnvironment(log, true)
    } catch (Exception e) {
      skip(e.toString())
    }
  }
  
  public String app_def_pkg_path;

  @Before
  public void setupAppPkg() {
    if (app_def_pkg_path == null) {
      def pkgPath = folder.newFolder("testpkg")
      File zipFileName = new File(pkgPath, "appdef_1.zip").canonicalFile
      File metainfo = new File(new File(".").absoluteFile, "src/test/python/metainfo.xml");
      ZipArchiveOutputStream zipFile = new ZipArchiveOutputStream(new FileOutputStream(zipFileName));
      try {
        zipFile.putArchiveEntry(new ZipArchiveEntry(metainfo.name));
        IOUtils.copy(new FileInputStream(metainfo), zipFile);
        zipFile.closeArchiveEntry();
      }
      finally {
        zipFile.close();
      }
      app_def_pkg_path = zipFileName.absolutePath
    }
  }

  @Override
  public String getTestConfigurationPath() {
    return "src/main/resources/" + CONF_RESOURCE;
  }

  @Override
  void setup() {
    super.setup()
    YarnConfiguration conf = testConfiguration
    checkTestAssumptions(conf)
  }

  @Override
  public String getArchiveKey() {
    return KEY_TEST_AGENT_TAR
  }

  /**
   * Get the key for the application
   * @return
   */
  @Override
  public String getApplicationHomeKey() {
    return KEY_TEST_AGENT_HOME
  }

  /**
   * Assume that HBase home is defined. This does not check that the
   * path is valid -that is expected to be a failure on tests that require
   * HBase home to be set.
   */

  public void checkTestAssumptions(YarnConfiguration conf) {
    assumeBoolOption(SLIDER_CONFIG, KEY_TEST_AGENT_ENABLED, true)
//    assumeArchiveDefined();
    assumeApplicationHome();
  }

  /**
   * Create an agent cluster
   * @param clustername
   * @param roles
   * @param extraArgs
   * @param deleteExistingData
   * @param blockUntilRunning
   * @return the cluster launcher
   */
  public ServiceLauncher<SliderClient> buildAgentCluster(
      String clustername,
      Map<String, Integer> roles,
      List<String> extraArgs,
      boolean deleteExistingData,
      boolean create,
      boolean blockUntilRunning) {


    YarnConfiguration conf = testConfiguration

    def clusterOps = [
        :
    ]

    return createOrBuildCluster(
        create ? SliderActions.ACTION_CREATE : SliderActions.ACTION_BUILD,
        clustername,
        roles,
        extraArgs,
        deleteExistingData,
        create && blockUntilRunning,
        clusterOps)
  }

  /**
   * Update an agent cluster
   * @param clustername
   * @param roles
   * @param extraArgs
   * @param deleteExistingData
   * @return the cluster launcher
   */
  public ServiceLauncher<SliderClient> updateAgentCluster(
      String clustername,
      Map<String, Integer> roles,
      List<String> extraArgs,
      boolean deleteExistingData) {

    YarnConfiguration conf = testConfiguration

    def clusterOps = [
        :
    ]

    return createOrBuildCluster(
        SliderActions.ACTION_UPDATE,
        clustername,
        roles,
        extraArgs,
        deleteExistingData,
        false,
        clusterOps)
  }

  public String getApplicationHome() {
    return "/"
  }
}
