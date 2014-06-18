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

package org.apache.slider.funtest.lifecycle

import groovy.util.logging.Slf4j
import org.apache.hadoop.fs.Path
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.CommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.apache.tools.zip.ZipEntry
import org.apache.tools.zip.ZipOutputStream
import org.junit.Assert
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.rules.TemporaryFolder
import org.junit.Rule

import org.apache.hadoop.security.AccessControlException


@Slf4j
class AgentCommandTestBase extends CommandTestBase
    implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  public static final boolean AGENTTESTS_ENABLED
  
  protected static String APP_RESOURCE = "../slider-core/src/test/app_packages/test_command_log/resources.json"
  protected static String APP_TEMPLATE = "../slider-core/src/test/app_packages/test_command_log/appConfig.json"
  protected static String APP_PKG_DIR = "../slider-core/src/test/app_packages/test_command_log/"
  protected static String AGENT_CONF = "../slider-agent/conf/agent.ini"
  protected static final File LOCAL_SLIDER_AGENT_TARGZ
  protected static final File LOCAL_AGENT_CONF

  protected static Path agentTarballPath;
  protected static Path appPkgPath;
  protected static Path agtIniPath;

  static {
    AGENTTESTS_ENABLED = SLIDER_CONFIG.getBoolean(KEY_TEST_AGENT_ENABLED, false)
    LOCAL_SLIDER_AGENT_TARGZ = new File(
        SLIDER_BIN_DIRECTORY,
        AGENT_SLIDER_GZ).canonicalFile
    LOCAL_AGENT_CONF = new File(AGENT_CONF).canonicalFile
  }

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @BeforeClass
  public static void setupAgent() {
    assumeAgentTestsEnabled()

    try {
      // Upload the agent tarball
      assume(LOCAL_SLIDER_AGENT_TARGZ.exists(), "Slider agent not found at $LOCAL_SLIDER_AGENT_TARGZ")
      agentTarballPath = new Path(clusterFS.homeDirectory, "slider-agent.tar.gz")
      Path localTarball = new Path(LOCAL_SLIDER_AGENT_TARGZ.toURI());
      clusterFS.copyFromLocalFile(false, true, localTarball, agentTarballPath)

      // Upload the agent.ini
      assume(LOCAL_AGENT_CONF.exists(), "Agent config not found at $LOCAL_AGENT_CONF")
      agtIniPath = new Path(clusterFS.homeDirectory, "agent.ini")
      Path localAgtIni = new Path(LOCAL_AGENT_CONF.toURI());
      clusterFS.copyFromLocalFile(false, true, localAgtIni, agtIniPath)
    } catch (AccessControlException ace) {
      log.info "No write access to test user home directory. " +
               "Ensure home directory exists and has correct permissions." + ace.getMessage()
      Assume.assumeTrue("Ensure home directory exists and has correct permissions for test user.", false)
    }
  }


  @Before
  public void setupApplicationPackage() {
    appPkgPath = new Path(clusterFS.homeDirectory, "cmd_log_app_pkg.zip")
    if (!clusterFS.exists(appPkgPath)) {
      clusterFS.delete(appPkgPath, false)
    }

    def pkgPath = folder.newFolder("testpkg")
    File zipFileName = new File(pkgPath, "cmd_log_app_pkg.zip").canonicalFile
    assume(new File(APP_PKG_DIR).exists(), "App pkg dir not found at $APP_PKG_DIR")

    zipDir(zipFileName.canonicalPath, APP_PKG_DIR)

    // Verify and upload the app pkg
    assume(zipFileName.exists(), "App pkg not found at $zipFileName")
    Path localAppPkg = new Path(zipFileName.toURI());
    clusterFS.copyFromLocalFile(false, true, localAppPkg, appPkgPath)
  }

  public static void assumeAgentTestsEnabled() {
    assumeFunctionalTestsEnabled()
    assume(AGENTTESTS_ENABLED, "Agent tests disabled")
  }

  public static void logShell(SliderShell shell) {
    for (String str in shell.out) {
      log.info str
    }
  }

  public static void assertComponentCount(String component, int count, SliderShell shell) {
    log.info("Asserting component count.")
    String entry = findLineEntry(shell, ["instances", component] as String[])
    log.info(entry)
    assert entry != null
    int instanceCount = 0
    int index = entry.indexOf("container_")
    while (index != -1) {
      instanceCount++;
      index = entry.indexOf("container_", index + 1)
    }

    assert instanceCount == count, 'Instance count for component did not match expected. Parsed: ' + entry
  }

  public static String findLineEntry(SliderShell shell, String[] locators) {
    int index = 0;
    for (String str in shell.out) {
      if (str.contains("\"" + locators[index] + "\"")) {
        if (locators.size() == index + 1) {
          return str;
        } else {
          index++;
        }
      }
    }

    return null;
  }

  public static boolean isAppRunning(String text, SliderShell shell) {
    boolean exists = false
    for (String str in shell.out) {
      if (str.contains(text)) {
        exists = true
      }
    }

    return exists
  }

  public static void addDir(File dirObj, ZipOutputStream zipFile, String prefix) {
    dirObj.eachFile() { file ->
      if (file.directory) {
        addDir(file, zipFile, prefix + file.name + File.separator)
      } else {
        log.info("Adding to zip - " + prefix + file.getName())
        zipFile.putNextEntry(new ZipEntry(prefix + file.getName()))
        file.eachByte(1024) { buffer, len -> zipFile.write(buffer, 0, len) }
        zipFile.closeEntry()
      }
    }
  }

  public static void zipDir(String zipFile, String dir) {
    File dirObj = new File(dir);
    ZipOutputStream out = new ZipOutputStream(new FileOutputStream(zipFile));
    log.info("Creating : " + zipFile);
    try {
      addDir(dirObj, out, "");
    }
    finally {
      out.close();
    }
  }
}
