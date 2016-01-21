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

package org.apache.slider.funtest.basic

import groovy.util.logging.Slf4j

import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

@Slf4j
public class ComponentConfigsInAppConfigShowUpOnAgentIT extends AgentCommandTestBase{
  
  static String CLUSTER = "test-application-with-component-config"
  private String APP_RESOURCE = "../slider-core/src/test/app_packages/test_command_log/resources_add_on_pkg.json"
  private String APP_TEMPLATE = "../slider-core/src/test/app_packages/test_component_config_in_app_config/appConfig.json"
  private String HDFS_FILENAME = "test_component_in_app_config";
  private String PACKAGE_DEF_DIR = "../slider-core/src/test/app_packages/test_component_config_in_app_config"
  private String ZIP_FILE = "test_component_config_in_app_config.zip"
  private String ZIP_DIR = "target/package-tmp/"
  private String TARGET_FILE = "/tmp/test_component_in_app_config.txt";
  
  @Before
  public void prepareCluster() {
    setupCluster(CLUSTER)
  }

  @After
  public void destroyCluster() {
    cleanup(CLUSTER)
  }

  @Test
  public void testComponentConfigsInAppConfigCanShowUpOnAgentSide() throws Throwable {
    describe("Create a cluster with an addon package that apply to one component")
    setupApplicationPackage()
    
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();

    SliderShell shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE,
        [],
        launchReportFile)

    logShell(shell)

    def appId = ensureYarnApplicationIsUp(launchReportFile)
    
    exists(0, CLUSTER)
    list(0, [CLUSTER])
    list(0, [""])
    list(0, [CLUSTER, ARG_LIVE])
    list(0, [CLUSTER, ARG_STATE, "running"])
    list(0, [ARG_LIVE])
    list(0, [ARG_STATE, "running"])
    status(0, CLUSTER)
    Thread.sleep(10000)
    verifyFileExists(TARGET_FILE)
  }
  
  public void setupApplicationPackage() {
    SliderUtils.zipFolder(new File(PACKAGE_DEF_DIR), new File(ZIP_DIR + ZIP_FILE))
    try {
      File zipFileName = new File(ZIP_DIR, ZIP_FILE).canonicalFile
      SliderShell shell = slider(EXIT_SUCCESS,
          [
              ACTION_INSTALL_PACKAGE,
              ARG_NAME, TEST_APP_PKG_NAME,
              ARG_PACKAGE, zipFileName.absolutePath,
              ARG_REPLACE_PKG
          ])
      logShell(shell)
      log.info "App pkg uploaded at home directory .slider/package/$TEST_APP_PKG_NAME/$ZIP_FILE"
    } catch (Exception e) {
      setup_failed = true
      throw e;
    }
    cleanupHdfsFile(TARGET_FILE)
  }
}
