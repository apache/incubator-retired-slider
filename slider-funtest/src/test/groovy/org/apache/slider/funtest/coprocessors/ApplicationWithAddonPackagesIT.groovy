/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.funtest.coprocessors

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.funtest.framework.SliderShell
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.CommandTestBase
import org.apache.slider.test.ContractTestUtils
import org.apache.slider.test.Outcome
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class ApplicationWithAddonPackagesIT extends AgentCommandTestBase{
  
  static String CLUSTER = "test-application-with-add-on"
  static String APP_RESOURCE2 = "../slider-core/src/test/app_packages/test_command_log/resources_add_on_pkg.json"
  static String ADD_ON_PACKAGE_ONE_COMPONENT = "../slider-core/src/test/app_packages/test_add_on_package/add-on-package-apply-on-one-component/"
  static String ADD_ON_PACKAGE_ALL_COMPONENT = "../slider-core/src/test/app_packages/test_add_on_package/add-on-package-apply-on-all-component/"
  static String ADD_ON_PACKAGE_MULTI_COMPONENT = "../slider-core/src/test/app_packages/test_add_on_package/add-on-package-apply-on-multi-component/"
  static String ADD_ON_PACKAGE_NO_COMPONENT = "../slider-core/src/test/app_packages/test_add_on_package/add-on-package-apply-on-no-component/"
  static String ADD_ON_PACKAGE_ALL_COMPONENT_PKG_NAME = "add-on-package-apply-on-all-component"
  static String ADD_ON_PACKAGE_ALL_COMPONENT_PKG_FILE = "target/package-tmp/add-on-package-apply-on-all-component.zip"
  static String ADD_ON_PACKAGE_ONE_COMPONENT_PKG_NAME = "add-on-package-apply-on-one-component"
  static String ADD_ON_PACKAGE_ONE_COMPONENT_PKG_FILE = "target/package-tmp/add-on-package-apply-on-one-component.zip"
  static String ADD_ON_PACKAGE_MULTI_COMPONENT_PKG_NAME = "add-on-package-apply-on-multi-component"
  static String ADD_ON_PACKAGE_MULTI_COMPONENT_PKG_FILE = "target/package-tmp/add-on-package-apply-on-multi-component.zip"
  static String ADD_ON_PACKAGE_NO_COMPONENT_PKG_NAME = "add-on-package-apply-on-no-component"
  static String ADD_ON_PACKAGE_NO_COMPONENT_PKG_FILE = "target/package-tmp/add-on-package-apply-on-no-component.zip"
  static String TARGET_FILE = "/tmp/test_slider.txt"
  public static final int FILE_EXISTS_TIMEOUT = 20000
  protected String APP_RESOURCE = getAppResource()
  protected String APP_TEMPLATE = getAppTemplate()
  
  @Before
  public void prepareCluster() {
    setupCluster(CLUSTER)
    cleanupHdfsFile(TARGET_FILE)
    clusterFS.mkdirs(new Path(TARGET_FILE).getParent())
  }

  @After
  public void destroyCluster() {
    cleanup(CLUSTER)
  }

  @Test
  public void testCreateApplicationWithOneAddonPackagesForOneComponent() throws Throwable {
    describe("Create a cluster with an addon package that apply to one component")
    Path addOnPackageOneComponentPkgNameFilePath = new Path(ADD_ON_PACKAGE_ONE_COMPONENT_PKG_FILE);
    Path addOnPackageOneComponentPath = new Path(ADD_ON_PACKAGE_ONE_COMPONENT);
    SliderUtils.zipFolder(new File(addOnPackageOneComponentPath.toString()), new File(addOnPackageOneComponentPkgNameFilePath.toString()))
    cleanupHdfsFile(TARGET_FILE)
    
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();
    
    SliderShell shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [Arguments.ARG_ADDON, ADD_ON_PACKAGE_ONE_COMPONENT_PKG_NAME, addOnPackageOneComponentPkgNameFilePath.toUri().toString()],
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
    awaitTargetFileExists()
  }

  protected void awaitTargetFileExists() {
    awaitFileExists(TARGET_FILE, FILE_EXISTS_TIMEOUT)
  }

  @Test
  public void testCreateApplicationWithOneAddonPackagesForNoComponents() throws Throwable {
    describe("Create a cluster with an addon package that apply to no components")
    Path addOnPackageNoComponentPkgNameFilePath = new Path(ADD_ON_PACKAGE_NO_COMPONENT_PKG_FILE);
    Path addOnPackageNoComponentPath = new Path(ADD_ON_PACKAGE_NO_COMPONENT);
    SliderUtils.zipFolder(new File(addOnPackageNoComponentPath.toString()), new File(addOnPackageNoComponentPkgNameFilePath.toString()))
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();

    //default waiting time too long, temporarily lower it
    int temp_holder = CommandTestBase.THAW_WAIT_TIME;
    CommandTestBase.THAW_WAIT_TIME = 100;
    SliderShell shell
    try {
      shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [Arguments.ARG_ADDON, ADD_ON_PACKAGE_NO_COMPONENT_PKG_NAME, addOnPackageNoComponentPkgNameFilePath.toString()],
        launchReportFile, false)
    } finally {
      CommandTestBase.THAW_WAIT_TIME = temp_holder;
    }

    logShell(shell)
    Thread.sleep(10000)
    //the Slider AM will fail while checking no components in metainfo.json of addon pkg
    // SLIDER-897 - Disabling this flaky assert. Have to re-write the test to
    // probably use build instead of create and assert on the return status.
//    exists(-1, CLUSTER)
    list(0, [CLUSTER])
  }
  
  @Test
  public void testCreateApplicationWithOneAddonPackagesForMultipleComponents() throws Throwable {
    describe("Create a cluster with an addon package that apply to multiple components")
    Path addOnPackageMultiComponentPkgNameFilePath = new Path(ADD_ON_PACKAGE_MULTI_COMPONENT_PKG_FILE);
    Path addOnPackageMultiComponentPath = new Path(ADD_ON_PACKAGE_MULTI_COMPONENT);
    SliderUtils.zipFolder(new File(addOnPackageMultiComponentPath.toString()), new File(addOnPackageMultiComponentPkgNameFilePath.toString()))
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();

    SliderShell shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [Arguments.ARG_ADDON, ADD_ON_PACKAGE_MULTI_COMPONENT_PKG_NAME, addOnPackageMultiComponentPkgNameFilePath.toString()],
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
    awaitTargetFileExists()

  }
  
  @Test
  public void testCreateApplicationWithOneAddonPackagesForAllComponents() throws Throwable {
    describe("Create a cluster with an addon package that apply to all components")
    Path addOnPackageAllComponentPkgNameFilePath = new Path(ADD_ON_PACKAGE_ALL_COMPONENT_PKG_FILE);
    Path addOnPackageAllComponentPath = new Path(ADD_ON_PACKAGE_ALL_COMPONENT);
    SliderUtils.zipFolder(new File(addOnPackageAllComponentPath.toString()), new File(addOnPackageAllComponentPkgNameFilePath.toString()))
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();

    SliderShell shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [Arguments.ARG_ADDON, ADD_ON_PACKAGE_ALL_COMPONENT_PKG_NAME, addOnPackageAllComponentPkgNameFilePath.toString()],
        launchReportFile)

    logShell(shell)
    ensureYarnApplicationIsUp(launchReportFile)
    
    exists(0, CLUSTER)
    list(0, [CLUSTER])
        list(0, [""])
        list(0, [CLUSTER, ARG_LIVE])
        list(0, [CLUSTER, ARG_STATE, "running"])
        list(0, [ARG_LIVE])
        list(0, [ARG_STATE, "running"])
        status(0, CLUSTER)
    awaitTargetFileExists()
  }

  @Test
  public void testCreateApplicationWithMultipleAddonPackages() throws Throwable {
    describe("Create a cluster with multiple addon packages")
    Path addOnPackageAllComponentPkgNameFilePath = new Path(ADD_ON_PACKAGE_ALL_COMPONENT_PKG_FILE);
    Path addOnPackageAllComponentPath = new Path(ADD_ON_PACKAGE_ALL_COMPONENT);
    Path addOnPackageOneComponentPkgNameFilePath = new Path(ADD_ON_PACKAGE_ONE_COMPONENT_PKG_FILE);
    Path addOnPackageOneComponentPath = new Path(ADD_ON_PACKAGE_ONE_COMPONENT);
    SliderUtils.zipFolder(new File(addOnPackageAllComponentPath.toString()), new File(addOnPackageAllComponentPkgNameFilePath.toString()))
    SliderUtils.zipFolder(new File(addOnPackageOneComponentPath.toString()), new File(addOnPackageOneComponentPkgNameFilePath.toString()))
    def clusterpath = buildClusterPath(CLUSTER)
    File launchReportFile = createTempJsonFile();

    SliderShell shell = createTemplatedSliderApplication(CLUSTER,
        APP_TEMPLATE,
        APP_RESOURCE2,
        [Arguments.ARG_ADDON, ADD_ON_PACKAGE_ALL_COMPONENT_PKG_NAME, addOnPackageAllComponentPkgNameFilePath.toString(),
          Arguments.ARG_ADDON, ADD_ON_PACKAGE_ONE_COMPONENT_PKG_NAME, addOnPackageOneComponentPkgNameFilePath.toString()],
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

    awaitTargetFileExists()
  }

  /**
   * Here to check file permissions and isolate failures due to that alone
   * @throws Throwable
   */
  @Test
  public void testCreateTargetFile() throws Throwable {
    ContractTestUtils.touch(clusterFS, new Path(TARGET_FILE))
    awaitTargetFileExists()
  }

}