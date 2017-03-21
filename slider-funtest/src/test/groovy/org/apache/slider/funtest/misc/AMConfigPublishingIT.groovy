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

package org.apache.slider.funtest.misc

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.funtest.ResourcePaths
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.junit.After
import org.junit.Before
import org.junit.Test

@CompileStatic
@Slf4j
public class AMConfigPublishingIT extends AgentCommandTestBase
implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  private static String DATE_LOGGER = "DATE_LOGGER"
  private static String APP_NAME = "am-config-publishing"
  private static String APP_METAINFO = ResourcePaths.AM_CONFIG_META
  private static String APP_RESOURCE = ResourcePaths.AM_CONFIG_RESOURCES
  private static String APP_TEMPLATE = ResourcePaths.AM_CONFIG_APPCONFIG
  private static String CLIENT_CONFIG = "../slider-core/src/test/app_packages/test_am_config/clientInstallConfig-default.json"
  private static String RESOURCE_DIR = "../slider-core/src/test/app_packages/test_am_config/resources"
  private static String TGZ_SOURCE = "../slider-core/src/test/app_packages/test_am_config/test_archive"
  private static String TGZ_FILE = "test_am_config_generation.tgz"
  private static String TGZ_DIR = "target/package-tmp/"

  private static String CLIENT_INSTALL_DIR = "target/am-config-client"

  private HashMap<String, String> files =
    ["client.json": """{  "clientkey" : "clientval"}""",
     "test.json": """{  "jsonkey" : "val1"}""",
     "test.xml": "<configuration><property><name>xmlkey</name><value>val2</value><source/></property></configuration>",
     "test-hadoop.xml": "<configuration><property><name>xmlkey</name><value>val3</value><source/></property></configuration>",
     "test.properties": "propkey=val4",
     "test.yaml": "yamlkey: val5",
     "test.template": "test templateval1 templateval2 content",
     "testenv": "test envval1 envval2 content",
     "testfile": "test archive contents"
  ]

  @Before
  public void prepareCluster() {
    setupCluster(APP_NAME)
  }

  @Before
  public void setupApplicationPackage() {
    File tgzFile = new File(TGZ_DIR + TGZ_FILE);
    SliderUtils.tarGzipFolder(new File(TGZ_SOURCE), tgzFile, null);
    try {
      tgzFile = tgzFile.canonicalFile
      SliderShell shell = slider(EXIT_SUCCESS,
        [
          ACTION_RESOURCE,
          ARG_INSTALL, ARG_RESOURCE, tgzFile.absolutePath,
          ARG_OVERWRITE
        ])
      logShell(shell)
      shell = slider(EXIT_SUCCESS,
        [
          ACTION_RESOURCE,
          ARG_INSTALL, ARG_RESOURCE, RESOURCE_DIR,
          ARG_DESTDIR, APP_NAME,
          ARG_OVERWRITE
        ])
      logShell(shell)
      log.info "Resources uploaded at home directory .slider/resources"
    } catch (Exception e) {
      setup_failed = true
      throw e;
    }
    File dir = new File(CLIENT_INSTALL_DIR)
    if (!dir.exists()) {
      dir.mkdir()
    }
  }


  @After
  public void destroyCluster() {
    cleanup(APP_NAME)
  }

  @Test
  public void testCreate() throws Throwable {
    assumeAgentTestsEnabled()

    describe APP_NAME

    def path = buildClusterPath(APP_NAME)
    assert !clusterFS.exists(path)

    createSliderApplicationMinPkg(APP_NAME, APP_METAINFO, APP_RESOURCE,
      APP_TEMPLATE)
    ensureApplicationIsUp(APP_NAME)

    expectLiveContainerCountReached(APP_NAME, DATE_LOGGER, 1,
      CONTAINER_LAUNCH_TIMEOUT)
    status(0, APP_NAME)

    SliderShell shell = slider(EXIT_SUCCESS,
      [
        ACTION_CLIENT, ARG_INSTALL,
        ARG_DEST, CLIENT_INSTALL_DIR,
        ARG_NAME, APP_NAME
      ])
    logShell(shell)

    for (Map.Entry<String, String> entry : files.entrySet()) {
      String name = entry.getKey();
      File file = new File(CLIENT_INSTALL_DIR + "/" + name)
      assert file.exists()
      String contents = file.text.replaceAll("(\\r|\\n)", "")
      assert contents.contains(entry.getValue()), "$name didn't contain value"
    }
  }

}
