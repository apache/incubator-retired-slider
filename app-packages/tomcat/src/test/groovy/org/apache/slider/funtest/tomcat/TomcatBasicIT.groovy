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
package org.apache.slider.funtest.tomcat

import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.registry.client.types.ServiceRecord
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderKeys
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.core.persist.ConfTreeSerDeser
import org.apache.slider.core.registry.docstore.PublishedConfiguration
import org.apache.slider.core.registry.retrieve.RegistryRetriever
import org.apache.slider.funtest.framework.SliderShell
import org.apache.slider.providers.agent.AgentKeys

import org.junit.Before
import org.junit.Test

import static org.apache.hadoop.registry.client.binding.RegistryUtils.currentUser
import static org.apache.hadoop.registry.client.binding.RegistryUtils.servicePath

@Slf4j
class TomcatBasicIT extends TomcatAgentCommandTestBase {
  protected ConfTree tree

  protected String getAppResource() {
    return sysprop("test.app.resources.dir") + "/resources.json"
  }

  protected String templateName() {
    return sysprop("test.app.resources.dir") + "/appConfig.json"
  }

  protected String getDefaultTemplate() {
    return sysprop("test.app.resources.dir") + "/appConfig-default.json"
  }

  protected String getAppTemplate() {
    String appTemplateFile = templateName()
    Configuration conf = new Configuration()
    FileSystem fs = FileSystem.getLocal(conf)
    InputStream stream = new FileInputStream(getDefaultTemplate())
    assert stream!=null, "Couldn't pull appConfig.json from app pkg"
    ConfTreeSerDeser c = new ConfTreeSerDeser()
    ConfTree t = c.fromStream(stream)
    t = modifyTemplate(t)
    log.info("Writing new template to {}", appTemplateFile)
    c.save(fs, new Path(appTemplateFile), t, true)
    return appTemplateFile
  }

  protected ConfTree modifyTemplate(ConfTree original) {
    // Set our custom WAR file in the appConfig and load it to the "cluster" filesystem
    Configuration conf = new Configuration()
    FileSystem fs = FileSystem.getLocal(conf)
    // The path from the local filesystem of the war (copied to target/test-classes from src/test/resources)
    Path sourcePath = fs.makeQualified(new Path(sysprop("project.build.directory"), "slider-test.war"))
    // Path in HDFS we'll put the WAR
    Path targetPath = clusterFS.makeQualified(new Path("/tmp/slider-test.war"))
    if (clusterFS.exists(targetPath)) {
      log.info("Deleting {}", targetPath)
      clusterFS.delete(targetPath, true)
    }
    log.info("Copying {} on {} to {} on {}", sourcePath, fs, targetPath, clusterFS)
    // Copy the file
    FileUtil.copy(fs, sourcePath, clusterFS, targetPath, false, true, clusterFS.getConf())
    FileStatus fileStatus = clusterFS.getFileStatus(targetPath)
    FileStatus origFileStatus = fs.getFileStatus(sourcePath)
    assert origFileStatus.getLen() == fileStatus.getLen(), "Expected file length to be the same in both source and destination"

    log.info("Setting {} to {} in appConfig.json", AgentKeys.APP_RESOURCES, targetPath.toString())
    // Set that path in the appConfig
    original.global.put(AgentKeys.APP_RESOURCES, targetPath.toString())

    return original
  }

  @Override
  public String getClusterName() {
    return "test_tomcat_basic"
  }

  protected Map<String, Integer> getRoleMap() {
    // must match the values in src/test/resources/resources.json
    return [
      "TOMCAT" : 1
    ];
  }

  @Test
  public void testTomcatClusterCreate() throws Throwable {

    describe getDescription()

    def path = buildClusterPath(getClusterName())
    assert !clusterFS.exists(path)

    SliderShell shell = createTemplatedSliderApplication(getClusterName(),
      APP_TEMPLATE, APP_RESOURCE, [])

    logShell(shell)

    ensureApplicationIsUp(getClusterName())

    //get a slider client against the cluster
    SliderClient sliderClient = bondToCluster(SLIDER_CONFIG, getClusterName())
    ClusterDescription cd = sliderClient.clusterDescription
    assert getClusterName() == cd.name

    log.info("Connected via Client {}", sliderClient.toString())

    //wait for the role counts to be reached, containers to get launched
    waitForRoleCount(sliderClient, getRoleMap(), TOMCAT_LAUNCH_WAIT_TIME)

    // Wait for Tomcat itself to boot
    sleep(TOMCAT_LAUNCH_WAIT_TIME);

    clusterLoadOperations(cd, sliderClient)
  }


  public String getDescription() {
    return "Create a working Tomcat cluster $clusterName"
  }

  public static PublishedConfiguration getExport(SliderClient sliderClient,
                                                 String clusterName,
                                                 String exportName) {
    String path = servicePath(currentUser(),
      SliderKeys.APP_TYPE,
      clusterName);
    ServiceRecord instance = sliderClient.resolve(path)
    RegistryRetriever retriever = new RegistryRetriever(
        sliderClient.config,
        instance)
    PublishedConfiguration configuration = retriever.retrieveConfiguration(
      retriever.getConfigurations(true), exportName, true)
    return configuration
  }

  /**
   * Override point for any cluster load operations
   */
  public void clusterLoadOperations(ClusterDescription cd, SliderClient sliderClient) {
    String httpAddress = getHttpAddress(sliderClient, getClusterName())
    assert httpAddress.startsWith("http://"), "Tomcat HTTP URL didn't have expected protocol"
    checkHttpAddress(httpAddress)
  }

  /**
   * Attempt to fetch the published Tomcat webserver address from the quicklinks.
   */
  public static String getHttpAddress(SliderClient sliderClient, String clusterName) {
    int tries = 5
    Exception caught;
    while (true) {
      try {
        PublishedConfiguration configuration = getExport(sliderClient,
          clusterName, "quicklinks")

        // must match name set in metainfo.xml
        String httpAddress = configuration.entries.get("HTTP")
        assertNotNull httpAddress
        log.info("Got exported server address: '{}'", httpAddress)
        return httpAddress
      } catch (Exception e) {
        caught = e;
        log.info("Got exception trying to read quicklinks", e)
        if (tries-- == 0) {
          break
        }
        sleep(20000)
      }
    }
    throw caught;
  }

  /**
   * Validate the URL for the Tomcat server, checking deployed web content.
   */
  public static void checkHttpAddress(String serverAddress) {
    // Check the Tomcat manager webapp
    String page = fetchWebPageRaisedErrorCodes(serverAddress);
    assert page != null, "Tomcat manager page null"
    assert page.length() > 100, "Tomcat manager page too short"
    assert page.contains("Apache Tomcat"), "Tomcat monitor page didn't contain expected text"

    // Check our test war we deployed too
    page = fetchWebPageRaisedErrorCodes(serverAddress + '/slider-test');
    assert page != null, "Could not fetch auto-deployed webapp"
    // The custom war file will return a plain-text response of "Success" for "/"
    assert page == "Success", "Did not find the expected content produced by the webapp"
  }
}
