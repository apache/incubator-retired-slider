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
package org.apache.slider.funtest.accumulo

import groovy.json.JsonSlurper
import org.apache.accumulo.core.conf.Property
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.slider.common.tools.SliderUtils
import org.apache.slider.core.conf.ConfTree
import org.apache.slider.funtest.framework.AgentUploads
import org.junit.Before

class AccumuloSSLTestBase extends AccumuloBasicIT {
  File trustStoreFile = new File(TEST_APP_PKG_DIR, "truststore.jks")
  File clientKeyStoreFile = new File(TEST_APP_PKG_DIR, "keystore.jks")

  protected String templateName() {
    return sysprop("test.app.resources.dir") + "/appConfig_ssl.json"
  }

  protected ConfTree modifyTemplate(ConfTree confTree) {
    confTree.global.put("site.accumulo-site.instance.rpc.ssl.enabled", "true")
    confTree.global.put("site.accumulo-site.instance.rpc.ssl.clientAuth", "true")
    String jks = confTree.global.get(PROVIDER_PROPERTY)
    def keys = confTree.credentials.get(jks)
    keys.add("rpc.javax.net.ssl.keyStorePassword")
    keys.add("rpc.javax.net.ssl.trustStorePassword")
    return confTree
  }

  @Override
  public String getClusterName() {
    return "test_ssl";
  }

  @Override
  public String getDescription() {
    return "Test enable SSL $clusterName"
  }

  @Before
  public void createCerts() {
    Path certDir = new Path(clusterFS.homeDirectory,
      tree.global.get("site.global.ssl_cert_dir"))
    if (clusterFS.exists(certDir)) {
      clusterFS.delete(certDir, true)
    }
    clusterFS.mkdirs(certDir)

    Configuration conf = loadSliderConf()
    String provider = tree.global.get(PROVIDER_PROPERTY)
    provider = provider.replace("hdfs/user",
      conf.get("fs.defaultFS").replace("://", "@") + "/user")
    System.out.println("provider after "+provider)
    File rootKeyStoreFile = new File(TEST_APP_PKG_DIR, "root.jks")

    if (!rootKeyStoreFile.exists() && !trustStoreFile.exists()) {
      CertUtil.createRootKeyPair(rootKeyStoreFile.toString(),
        Property.INSTANCE_SECRET.toString(), trustStoreFile.toString(),
        Property.RPC_SSL_TRUSTSTORE_PASSWORD.toString(), provider);
    }

    AgentUploads agentUploads = new AgentUploads(SLIDER_CONFIG)
    agentUploads.uploader.copyIfOutOfDate(trustStoreFile, new Path(certDir,
      "truststore.jks"), false)

    for (node in getNodeList(conf)) {
      File keyStoreFile = new File(TEST_APP_PKG_DIR, node + ".jks")
      if (!keyStoreFile.exists()) {
        CertUtil.createServerKeyPair(keyStoreFile.toString(),
          Property.RPC_SSL_KEYSTORE_PASSWORD.toString(),
          rootKeyStoreFile.toString(), Property.INSTANCE_SECRET.toString(),
          provider, node);
      }
      agentUploads.uploader.copyIfOutOfDate(keyStoreFile, new Path(certDir,
        node + ".jks"), false)
    }

    if (!clientKeyStoreFile.exists()) {
      CertUtil.createServerKeyPair(clientKeyStoreFile.toString(),
        Property.RPC_SSL_KEYSTORE_PASSWORD.toString(),
        rootKeyStoreFile.toString(), Property.INSTANCE_SECRET.toString(),
        provider, InetAddress.getLocalHost().getHostName());
    }
  }

  def getNodeList(Configuration conf) {
    String address = conf.get(YarnConfiguration.RM_WEBAPP_ADDRESS) +
      "/ws/v1/cluster/nodes"
    address = address.replace("0.0.0.0", conf.get(YarnConfiguration.RM_ADDRESS)
      .split(":")[0])
    def slurper = new JsonSlurper()
    def result = slurper.parse(new URL("http://" + address))
    def hosts = []
    for (host in result.nodes.node) {
      hosts.add(host.nodeHostName)
    }
    return hosts.unique()
  }
}
