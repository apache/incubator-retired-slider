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
import org.apache.accumulo.examples.simple.helloworld.InsertWithBatchWriter
import org.apache.accumulo.examples.simple.helloworld.ReadData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.registry.client.api.RegistryConstants
import org.apache.hadoop.security.ProviderUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.alias.CredentialProvider
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.slider.api.ClusterDescription
import org.apache.slider.client.SliderClient
import org.apache.slider.funtest.framework.AccumuloSliderShell
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.codehaus.jettison.json.JSONObject
import org.junit.Before
import org.junit.BeforeClass

import java.nio.charset.Charset
import java.nio.file.Files

@Slf4j
class AccumuloScriptSSLIT extends AccumuloSSLTestBase {
  public static final String CLIENT_INSTALL_DIR = sysprop("test.client.install.dir")
  public static final String CLIENT_HOME_DIR = sysprop("test.client.home.dir")
  public static final String CLIENT_INSTALL_CONF = sysprop("test.app.resources.dir") + "/clientInstallConfig-ssl-test.json"

  public static final File ACCUMULO_SCRIPT = new File(CLIENT_HOME_DIR + "/bin", "accumulo").canonicalFile
  public static final File ACCUMULO_CLIENT_CONF = new File(CLIENT_HOME_DIR + "/conf", "client.conf").canonicalFile

  protected String jks
  protected Path jksPath

  @Override
  public String getClusterName() {
    return "test_script_ssl";
  }

  @Override
  public String getDescription() {
    return "Test accumulo-slider client script with ssl $clusterName"
  }

  @BeforeClass
  public static void setShell() {
    AccumuloSliderShell.setEnv("SLIDER_HOME", SLIDER_TAR_DIR)
    AccumuloSliderShell.setEnv("SLIDER_CONF_DIR", SLIDER_CONF_DIR)
    File dir = new File(CLIENT_INSTALL_DIR)
    if (!dir.exists()) {
      dir.mkdir()
    }
  }

  @Before
  public void readProviderLocation() {
    byte[] encoded = Files.readAllBytes(new File(CLIENT_INSTALL_CONF).toPath());
    JSONObject config = new JSONObject(new String(encoded, Charset.defaultCharset()))
    JSONObject global = config.getJSONObject("global")
    jks = global.get("general.security.credential.provider.paths")
    jks = jks.replace("{app_name}", getClusterName())
    jks = jks.replace("{app_user}", UserGroupInformation.getCurrentUser().getShortUserName())
    jksPath = ProviderUtils.unnestUri(new URI(jks))
  }

  public static AccumuloSliderShell accumulo(String cmd) {
    AccumuloSliderShell.scriptFile = ACCUMULO_SCRIPT
    return AccumuloSliderShell.run(0, cmd)
  }

  public void createClientKeyStore() {
    Configuration conf = loadSliderConf()
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, jks)
    CredentialProvider provider =
      CredentialProviderFactory.getProviders(conf).get(0)
    provider.createCredentialEntry(Property.RPC_SSL_KEYSTORE_PASSWORD
      .toString(), KEY_PASS.toCharArray())
    provider.createCredentialEntry(Property.RPC_SSL_TRUSTSTORE_PASSWORD
      .toString(), TRUST_PASS.toCharArray())
    provider.flush()
    assert clusterFS.exists(jksPath), "jks $jks not created"
    log.info("Created credential provider $jks for test")
  }

  @Override
  public void clusterLoadOperations(ClusterDescription cd, SliderClient sliderClient) {
    if (clusterFS.exists(jksPath)) {
      clusterFS.delete(jksPath, false)
    }

    // this throws an error because the store passwords haven't been set up
    SliderShell shell = slider(EXIT_EXCEPTION_THROWN,
      [
        ACTION_CLIENT, ARG_INSTALL,
        ARG_PACKAGE, TEST_APP_PKG_DIR+"/"+TEST_APP_PKG_FILE,
        ARG_DEST, CLIENT_INSTALL_DIR,
        ARG_CONFIG, CLIENT_INSTALL_CONF,
        ARG_NAME, getClusterName()
      ])
    logShell(shell)

    // set up the store passwords
    createClientKeyStore()

    // now install the client
    shell = slider(EXIT_SUCCESS,
      [
        ACTION_CLIENT, ARG_INSTALL,
        ARG_PACKAGE, TEST_APP_PKG_DIR+"/"+TEST_APP_PKG_FILE,
        ARG_DEST, CLIENT_INSTALL_DIR,
        ARG_CONFIG, CLIENT_INSTALL_CONF,
        ARG_NAME, getClusterName()
      ])
    logShell(shell)

    assert ACCUMULO_CLIENT_CONF.exists(), "client.conf did not exist after " +
      "client install command"

    accumulo("shell -u $USER -p $PASSWORD -e tables")
    accumulo("shell -u $USER -p $PASSWORD -f " + sysprop("test.app.resources.dir") + "/test_shell_cmd_file")

    String zookeepers = SLIDER_CONFIG.get(
      RegistryConstants.KEY_REGISTRY_ZK_QUORUM,
      FuntestProperties.DEFAULT_SLIDER_ZK_HOSTS)
    String instance = tree.global.get("site.client.instance.name")
    accumulo("shell -u $USER -p $PASSWORD -e \"createtable test2\"")
    accumulo(InsertWithBatchWriter.class.getName() + " -i $instance -z " +
      "$zookeepers -u $USER -p $PASSWORD -t test2")
    accumulo(ReadData.class.getName() + " -i $instance -z $zookeepers -u " +
      "$USER -p $PASSWORD -t test2 --startKey row_0 --endKey row_101")
  }
}
