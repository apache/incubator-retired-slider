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

package org.apache.slider.funtest.framework

import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.records.YarnApplicationState
import org.apache.hadoop.fs.FileSystem;
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.tools.zip.ZipEntry
import org.apache.tools.zip.ZipOutputStream
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.rules.TemporaryFolder

@Slf4j
class AgentCommandTestBase extends CommandTestBase
implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  public static final boolean AGENTTESTS_ENABLED
  public static final boolean AGENTTESTS_QUEUE_LABELED_DEFINED
  public static final boolean AGENTTESTS_LABELS_RED_BLUE_DEFINED
  public static final boolean AGENTTESTS_AM_FAILURES_ENABLED
  private static String TEST_APP_PKG_DIR_PROP = "test.app.pkg.dir"
  private static String TEST_APP_PKG_FILE_PROP = "test.app.pkg.file"
  private static String TEST_APP_PKG_NAME_PROP = "test.app.pkg.name"
  private static String TEST_APP_RESOURCE = "test.app.resource"
  private static String TEST_APP_TEMPLATE = "test.app.template"


  protected String APP_RESOURCE = getAppResource()
  protected String APP_TEMPLATE = getAppTemplate()
  public static final String TEST_APP_PKG_DIR = sysprop(TEST_APP_PKG_DIR_PROP)
  public static final String TEST_APP_PKG_FILE = sysprop(TEST_APP_PKG_FILE_PROP)
  public static final String TEST_APP_PKG_NAME = sysprop(TEST_APP_PKG_NAME_PROP)


  protected static Path agentTarballPath;
  protected static Path appPkgPath;
  protected static Path agtIniPath;

  protected static boolean setup_failed

  static {
    AGENTTESTS_ENABLED = SLIDER_CONFIG.getBoolean(KEY_TEST_AGENT_ENABLED, false)
    AGENTTESTS_QUEUE_LABELED_DEFINED =
        SLIDER_CONFIG.getBoolean(KEY_AGENTTESTS_QUEUE_LABELED_DEFINED, false)
    AGENTTESTS_LABELS_RED_BLUE_DEFINED =
        SLIDER_CONFIG.getBoolean(KEY_AGENTTESTS_LABELS_RED_BLUE_DEFINED, false)
    AGENTTESTS_AM_FAILURES_ENABLED = 
        SLIDER_CONFIG.getBoolean(KEY_AGENTTESTS_AM_FAILURES_ENABLED,
            AGENTTESTS_ENABLED)
  }

  protected String getAppResource() {
    return sysprop(TEST_APP_RESOURCE)
  }

  protected String getAppTemplate() {
    return sysprop(TEST_APP_TEMPLATE)
  }

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  public static void assumeAgentTestsEnabled() {
    assume(AGENTTESTS_ENABLED, "Agent tests disabled")
  }

  public static void assumeQueueNamedLabelDefined() {
    assume(AGENTTESTS_QUEUE_LABELED_DEFINED, "Custom queue named labeled is not defined")
  }

  public static void assumeLabelsRedAndBlueAdded() {
    assume(AGENTTESTS_LABELS_RED_BLUE_DEFINED, "Custom node labels not defined")
  }

  public static void assumeAmFailureTestsEnabled() {
    assume(AGENTTESTS_AM_FAILURES_ENABLED, "AM failure tests are disabled")
  }

  @BeforeClass
  public static void setupAgent() {
    assumeAgentTestsEnabled()
    def uploader = new FileUploader(SLIDER_CONFIG, 
        UserGroupInformation.currentUser)
    uploader.mkHomeDir();
  }

  @Before
  public void setupApplicationPackage() {
    try {
      File zipFileName = new File(TEST_APP_PKG_DIR, TEST_APP_PKG_FILE).canonicalFile
      SliderShell shell = slider(EXIT_SUCCESS,
          [
              ACTION_INSTALL_PACKAGE,
              ARG_NAME, TEST_APP_PKG_NAME,
              ARG_PACKAGE, zipFileName.absolutePath,
              ARG_REPLACE_PKG
          ])
      logShell(shell)
      log.info "App pkg uploaded at home directory .slider/package/$TEST_APP_PKG_NAME/$TEST_APP_PKG_FILE"
    } catch (Exception e) {
      setup_failed = true
      throw e;
    }
  }

  public static String findLineEntry(SliderShell shell, String[] locaters) {
    return shell.findLineEntry(locaters)
  }

  public static boolean containsString(SliderShell shell, String lookThisUp, int n = 1) {
    return shell.outputContains(lookThisUp, n)
  }

  public static String findLineEntryValue(SliderShell shell, String[] locaters) {
    return shell.findLineEntryValue(locaters)
  }

  public static void addDir(File dirObj, ZipOutputStream zipFile, String prefix) {
    dirObj.eachFile() {File file ->
      if (file.directory) {
        addDir(file, zipFile, prefix + file.name + File.separator)
      } else {
        log.info("Adding to zip - " + prefix + file.getName())
        zipFile.putNextEntry(new ZipEntry(prefix + file.getName()))
        file.eachByte(1024) {
          byte[] buffer, int len -> zipFile.write(buffer, 0, len) }
        zipFile.closeEntry()
      }
    }
  }
  
  protected static void verifyFileExist(String filePath){
    try{
      Path pt = new Path(filePath);
      def uploader = new FileUploader(SLIDER_CONFIG,
        UserGroupInformation.currentUser)
      FileSystem fs = uploader.getFileSystem();
      assert fs.exists(pt);
    }catch(IOException e){
      log.error("IOException during verifying file exist " + e.toString());
    }
  }
  
  protected static void cleanupHdfsFile(String filePath){
    try{
      Path pt = new Path(filePath);
      def uploader = new FileUploader(SLIDER_CONFIG,
        UserGroupInformation.currentUser)
      FileSystem fs = uploader.getFileSystem();
      if( fs.exists(pt)){
        fs.delete(pt, false);
      }      
    }catch(IOException e){
      log.error("IOException during deleting file: " + e.toString());
    }
  }

  protected void cleanup(String applicationName) throws Throwable {
    if (setup_failed) {
      // cleanup probably won't work if setup failed
      return
    }

    describe "Teardown app instance " + applicationName
    // forced freeze with wait
    SliderShell shell
    shell = stop(applicationName)
    teardown(applicationName)

    shell = slider([
        ACTION_DESTROY,
        applicationName,
        ARG_FORCE])

    if (shell.ret != 0 && shell.ret != EXIT_UNKNOWN_INSTANCE) {
      assertExitCode(shell, 0)
    }
  }

  /**
   * Assert that the application is running (i.e in state
   * {@link YarnApplicationState#RUNNING})
   * @param appId application ID
   */
  def assertAppRunning(String appId) {
    assertInYarnState(appId, YarnApplicationState.RUNNING)
  }
}
