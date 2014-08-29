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
  private static String TEST_APP_PKG_DIR_PROP = "test.app.pkg.dir"
  private static String TEST_APP_PKG_FILE_PROP = "test.app.pkg.file"
  private static String TEST_APP_RESOURCE = "test.app.resource"
  private static String TEST_APP_TEMPLATE = "test.app.template"


  protected String APP_RESOURCE = sysprop(TEST_APP_RESOURCE)
  protected String APP_TEMPLATE = sysprop(TEST_APP_TEMPLATE)
  public static final String TEST_APP_PKG_DIR = sysprop(TEST_APP_PKG_DIR_PROP)
  public static final String TEST_APP_PKG_FILE = sysprop(TEST_APP_PKG_FILE_PROP)


  protected static Path agentTarballPath;
  protected static Path appPkgPath;
  protected static Path agtIniPath;

  protected static boolean setup_failed

  static {
    AGENTTESTS_ENABLED = SLIDER_CONFIG.getBoolean(KEY_TEST_AGENT_ENABLED, false)
  }

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  public static void assumeAgentTestsEnabled() {
    assume(AGENTTESTS_ENABLED, "Agent tests disabled")
  }

  @BeforeClass
  public static void setupAgent() {
    assumeAgentTestsEnabled()

  }

  @Before
  public void uploadAgentTarball() {
    def agentUploads = new AgentUploads(SLIDER_CONFIG)
    (agentTarballPath, agtIniPath) =
        agentUploads.uploadAgentFiles(SLIDER_TAR_DIRECTORY, false)
  }


  @Before
  public void setupApplicationPackage() {
    try {
      AgentUploads agentUploads = new AgentUploads(SLIDER_CONFIG)
      agentUploads.uploader.mkHomeDir()

      appPkgPath = new Path(clusterFS.homeDirectory, TEST_APP_PKG_FILE)
      if (clusterFS.exists(appPkgPath)) {
        clusterFS.delete(appPkgPath, false)
        log.info "Existing app pkg deleted from $appPkgPath"
      }

      File zipFileName = new File(TEST_APP_PKG_DIR, TEST_APP_PKG_FILE).canonicalFile
      agentUploads.uploader.copyIfOutOfDate(zipFileName, appPkgPath, false)
      assert clusterFS.exists(appPkgPath), "App pkg not uploaded to $appPkgPath"
      log.info "App pkg uploaded at $appPkgPath"
    } catch (Exception e) {
      setup_failed = true
      fail("Setup failed "+e)
    }
  }

  public static void logShell(SliderShell shell) {
    for (String str in shell.out) {
      log.info str
    }
    for (String str in shell.err) {
      log.error str
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

  public static String findLineEntry(SliderShell shell, String[] locaters) {
    int index = 0;
    for (String str in shell.out) {
      if (str.contains("\"" + locaters[index] + "\"")) {
        if (locaters.size() == index + 1) {
          return str;
        } else {
          index++;
        }
      }
    }

    return null;
  }

  public static String findLineEntryValue(SliderShell shell, String[] locaters) {
    String line = findLineEntry(shell, locaters);

    if (line != null) {
      log.info("Parsing {} for value.", line)
      int dividerIndex = line.indexOf(":");
      if (dividerIndex > 0) {
        String value = line.substring(dividerIndex + 1).trim()
        if (value.endsWith(",")) {
          value = value.subSequence(0, value.length() - 1)
        }
        return value;
      }
    }
    return null;
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

  protected void cleanup(String applicationName) throws Throwable {
    if (setup_failed) {
      // cleanup probably won't work if setup failed
      return
    }

    log.info "Cleaning app instance, if exists, by name " + applicationName
    teardown(applicationName)

    // sleep till the instance is frozen
    sleep(1000 * 3)

    SliderShell shell = slider([
        ACTION_DESTROY,
        applicationName])

    if (shell.ret != 0 && shell.ret != EXIT_UNKNOWN_INSTANCE) {
      logShell(shell)
      assert fail("Old cluster either should not exist or should get destroyed.")
    }
  }
}
