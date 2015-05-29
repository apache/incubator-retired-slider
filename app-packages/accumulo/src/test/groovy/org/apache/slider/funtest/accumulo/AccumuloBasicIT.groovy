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
package org.apache.slider.funtest.accumulo

import groovy.util.logging.Slf4j
import org.apache.accumulo.core.conf.Property
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.ProviderUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.alias.CredentialProvider
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.hadoop.registry.client.types.ServiceRecord
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.common.SliderKeys
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.core.persist.ConfTreeSerDeser
import org.apache.slider.core.registry.docstore.PublishedConfiguration
import org.apache.slider.core.registry.retrieve.RegistryRetriever
import org.apache.slider.funtest.framework.SliderShell

import org.junit.Before
import org.junit.Test

import static org.apache.hadoop.registry.client.binding.RegistryUtils.currentUser
import static org.apache.hadoop.registry.client.binding.RegistryUtils.servicePath

@Slf4j
class AccumuloBasicIT extends AccumuloAgentCommandTestBase {
  protected static final String PROVIDER_PROPERTY = "site.accumulo-site." +
    Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS
  protected static final String KEY_PASS = "keypass"
  protected static final String TRUST_PASS = "trustpass"
  protected ConfTree tree

  protected String getAppResource() {
    return sysprop("test.app.resources.dir") + "/resources.json"
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
    c.save(fs, new Path(appTemplateFile), t, true)
    return appTemplateFile
  }

  protected String templateName() {
    return sysprop("test.app.resources.dir") + "/appConfig.json"
  }

  protected ConfTree modifyTemplate(ConfTree original) {
    return original
  }

  @Before
  public void createKeyStore() {
    ConfTreeSerDeser c = new ConfTreeSerDeser()
    tree = c.fromFile(new File(APP_TEMPLATE))
    assume tree.credentials.size() > 0, "No credentials requested, " +
      "skipping creation of credentials"
    SliderClient.replaceTokens(tree, UserGroupInformation.getCurrentUser()
      .getShortUserName(), getClusterName())
    String jks = tree.global.get(PROVIDER_PROPERTY)
    def keys = tree.credentials.get(jks)
    assert keys!=null, "jks specified in $PROVIDER_PROPERTY wasn't requested " +
      "in credentials"
    Path jksPath = ProviderUtils.unnestUri(new URI(jks))
    if (clusterFS.exists(jksPath)) {
      clusterFS.delete(jksPath, false)
    }
    Configuration conf = loadSliderConf()
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jks)
    CredentialProvider provider =
      CredentialProviderFactory.getProviders(conf).get(0)
    // root initial password and trace password will be initialized at runtime
    provider.createCredentialEntry(Property.INSTANCE_SECRET.toString(),
      INSTANCE_SECRET.toCharArray())
    provider.createCredentialEntry(Property.RPC_SSL_KEYSTORE_PASSWORD
      .toString(), KEY_PASS.toCharArray())
    provider.createCredentialEntry(Property.RPC_SSL_TRUSTSTORE_PASSWORD
      .toString(), TRUST_PASS.toCharArray())
    provider.createCredentialEntry(Property.MONITOR_SSL_KEYSTOREPASS
      .toString(), KEY_PASS.toCharArray())
    provider.createCredentialEntry(Property.MONITOR_SSL_TRUSTSTOREPASS
      .toString(), TRUST_PASS.toCharArray())
    provider.flush()
    assert clusterFS.exists(jksPath), "jks $jks not created"
    log.info("Created credential provider $jks for test")
  }

  @Override
  public String getClusterName() {
    return "test_accumulo_basic"
  }

  protected Map<String, Integer> getRoleMap() {
    // must match the values in src/test/resources/resources.json
    return [
      "ACCUMULO_MASTER" : 1,
      "ACCUMULO_TSERVER" : 2,
      "ACCUMULO_MONITOR": 1,
      "ACCUMULO_GC": 0,
      "ACCUMULO_TRACER" : 0,
      "ACCUMULO_PROXY" : 0
    ];
  }

  @Test
  public void testAccumuloClusterCreate() throws Throwable {

    describe getDescription()

    def path = buildClusterPath(getClusterName())
    assert !clusterFS.exists(path)

    SliderShell shell = createTemplatedSliderApplication(getClusterName(),
      APP_TEMPLATE, APP_RESOURCE,
      ["<", sysprop("test.app.resources.dir") + "/test_password_file"])

    logShell(shell)

    ensureApplicationIsUp(getClusterName())

    //get a slider client against the cluster
    SliderClient sliderClient = bondToCluster(SLIDER_CONFIG, getClusterName())
    ClusterDescription cd = sliderClient.clusterDescription
    assert getClusterName() == cd.name

    log.info("Connected via Client {}", sliderClient.toString())

    //wait for the role counts to be reached
    waitForRoleCount(sliderClient, getRoleMap(), ACCUMULO_LAUNCH_WAIT_TIME)

    sleep(ACCUMULO_GO_LIVE_TIME)

    clusterLoadOperations(cd, sliderClient)
  }


  public String getDescription() {
    return "Create a working Accumulo cluster $clusterName"
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

  public static String getMonitorUrl(SliderClient sliderClient, String clusterName) {
    int tries = 5
    Exception caught;
    while (true) {
      try {
        PublishedConfiguration configuration = getExport(sliderClient,
          clusterName, "quicklinks")

        // must match name set in metainfo.xml
        String monitorUrl = configuration.entries.get("org.apache.slider.monitor")
        assertNotNull monitorUrl
        return monitorUrl
      } catch (Exception e) {
        caught = e;
        log.info("Got exception trying to read quicklinks")
        if (tries-- == 0) {
          break
        }
        sleep(20000)
      }
    }
    throw caught;
  }

  public static void checkMonitorPage(String monitorUrl) {
    String monitor = fetchWebPageRaisedErrorCodes(monitorUrl);
    assert monitor != null, "Monitor page null"
    assert monitor.length() > 100, "Monitor page too short"
    assert monitor.contains("Accumulo Overview"), "Monitor page didn't contain expected text"
  }

  /**
   * Override point for any cluster load operations
   */
  public void clusterLoadOperations(ClusterDescription cd, SliderClient sliderClient) {
    String monitorUrl = getMonitorUrl(sliderClient, getClusterName())
    assert monitorUrl.startsWith("http://"), "Monitor URL didn't have expected protocol"
    checkMonitorPage(monitorUrl)
  }
}
