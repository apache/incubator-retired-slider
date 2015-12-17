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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.ProviderUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.alias.CredentialProvider
import org.apache.hadoop.security.alias.CredentialProviderFactory
import org.apache.slider.common.SliderExitCodes
import org.apache.slider.common.params.Arguments
import org.apache.slider.common.params.SliderActions
import org.apache.slider.funtest.framework.AgentCommandTestBase
import org.apache.slider.funtest.framework.FuntestProperties
import org.apache.slider.funtest.framework.SliderShell
import org.apache.slider.test.ContractTestUtils
import org.junit.After
import org.junit.Test

import javax.net.ssl.TrustManager
import javax.net.ssl.TrustManagerFactory
import javax.net.ssl.X509TrustManager
import java.security.KeyStore
import java.security.KeyStoreException
import java.security.NoSuchAlgorithmException
import java.security.Principal
import java.security.cert.Certificate
import java.security.cert.CertificateException
import java.security.cert.X509Certificate
import com.google.common.io.Files

@CompileStatic
@Slf4j
public class AMClientCertStoreRetrievalIT extends AgentCommandTestBase
implements FuntestProperties, Arguments, SliderExitCodes, SliderActions {

  private static String COMMAND_LOGGER = "COMMAND_LOGGER"
  private static String APPLICATION_NAME = "certs-retrieval"


  @After
  public void destroyCluster() {
    cleanup(APPLICATION_NAME)
  }

  private static KeyStore loadKeystoreFromFile(String filename,
                                               char[] password) {
    FileInputStream is = null
    try {
      is = new FileInputStream(filename)
      KeyStore keystore = KeyStore.getInstance("pkcs12")
      keystore.load(is, password)
      return keystore
    } finally {
      if (is != null) {
        is.close()
      }
    }
  }

  @Test
  public void testRetrieveCertificateStores() throws Throwable {
    cleanup(APPLICATION_NAME)
    File launchReportFile = createTempJsonFile();

    SliderShell shell = createTemplatedSliderApplication(
        APPLICATION_NAME, APP_TEMPLATE, APP_RESOURCE,
        [],
        launchReportFile)
    logShell(shell)

    ensureYarnApplicationIsUp(launchReportFile)
    expectContainerRequestedCountReached(APPLICATION_NAME, COMMAND_LOGGER, 1,
        CONTAINER_LAUNCH_TIMEOUT)

    def cd = assertContainersLive(APPLICATION_NAME, COMMAND_LOGGER, 1)
    def loggerInstances = cd.instances[COMMAND_LOGGER]
    assert loggerInstances.size() == 1

    def loggerStats = cd.statistics[COMMAND_LOGGER]

    assert loggerStats["containers.requested"] == 1
    assert loggerStats["containers.live"] == 1

    final File myTempDir = Files.createTempDir();
    final String keystoreName = myTempDir.canonicalPath + File.separator + "test.keystore"
    final String password = "welcome";

    // ensure file doesn't exist
    def keystoreFile = new File(keystoreName)
    keystoreFile.delete();

    shell = slider(EXIT_SUCCESS,
                   [
                       ACTION_CLIENT,
                       ARG_GETCERTSTORE,
                       ARG_KEYSTORE, keystoreName,
                       ARG_NAME, APPLICATION_NAME,
                       ARG_PASSWORD, password
                   ])

    assert keystoreFile.exists()

    KeyStore keystore = loadKeystoreFromFile(keystoreName, password.toCharArray())

    validateKeystore(keystore)

    final def truststoreName = myTempDir.canonicalPath + File.separator + "test.truststore"
    // ensure file doesn't exist
    final def trustStoreFile = new File(truststoreName)
    trustStoreFile.delete();

    shell = slider(EXIT_SUCCESS,
                   [
                       ACTION_CLIENT,
                       ARG_GETCERTSTORE,
                       ARG_TRUSTSTORE, truststoreName,
                       ARG_NAME, APPLICATION_NAME,
                       ARG_PASSWORD, password
                   ])

    assert trustStoreFile.exists()

    KeyStore truststore = loadKeystoreFromFile(truststoreName, password.toCharArray())

    validateTruststore(keystore, truststore);

    // test retrieving using credential provider to provide password
    String alias = "alias.for.password"
    String providerString = "jceks://hdfs/user/" +
      UserGroupInformation.getCurrentUser().getShortUserName() + "/test-" +
      APPLICATION_NAME + ".jceks"
    Path providerPath = ProviderUtils.unnestUri(new URI(providerString))
    if (clusterFS.exists(providerPath)) {
      clusterFS.delete(providerPath, false)
    }

    Configuration conf = loadSliderConf()
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH, providerString)
    CredentialProvider provider =
      CredentialProviderFactory.getProviders(conf).get(0)
    provider.createCredentialEntry(alias, password.toCharArray())
    provider.flush()
    ContractTestUtils.assertPathExists(clusterFS, "jks $providerString not created", providerPath)
    log.info("Created credential provider $providerString for test")

    // ensure file doesn't exist
    keystoreFile.delete();

    shell = slider(EXIT_SUCCESS,
      [
        ACTION_CLIENT,
        ARG_GETCERTSTORE,
        ARG_KEYSTORE, keystoreName,
        ARG_NAME, APPLICATION_NAME,
        ARG_ALIAS, alias,
        ARG_PROVIDER, providerString
      ])

    assert keystoreFile.exists()

    keystore = loadKeystoreFromFile(keystoreName, password.toCharArray())

    validateKeystore(keystore)

    // ensure file doesn't exist
    trustStoreFile.delete();

    shell = slider(EXIT_SUCCESS,
      [
        ACTION_CLIENT,
        ARG_GETCERTSTORE,
        ARG_TRUSTSTORE, truststoreName,
        ARG_NAME, APPLICATION_NAME,
        ARG_ALIAS, alias,
        ARG_PROVIDER, providerString
      ])

    assert trustStoreFile.exists()

    truststore = loadKeystoreFromFile(truststoreName, password.toCharArray())

    validateTruststore(keystore, truststore);

  }

  private static void validateKeystore(KeyStore keystore) {
    Certificate certificate = keystore.getCertificate(
      keystore.aliases().nextElement());
    assert certificate

    String hostname = InetAddress.localHost.canonicalHostName;

    if (certificate instanceof X509Certificate) {
      X509Certificate x509cert = (X509Certificate) certificate;

      // Get subject
      Principal principal = x509cert.getSubjectDN();
      String subjectDn = principal.getName();
      assert subjectDn == "CN=" + hostname + ", OU=" + APPLICATION_NAME + ", OU=client"

    }
  }

  private static void validateTruststore(KeyStore keystore, KeyStore truststore)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
    // obtain server cert
    Certificate certificate = keystore.getCertificate(
        keystore.aliases().nextElement());
    assert certificate

    // validate keystore cert using trust store
      TrustManagerFactory trustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(truststore);

      for (TrustManager trustManager: trustManagerFactory.getTrustManagers()) {
        if (trustManager instanceof X509TrustManager) {
          X509TrustManager x509TrustManager = (X509TrustManager)trustManager;
          x509TrustManager.checkServerTrusted(
              [(X509Certificate) certificate] as X509Certificate[],
              "RSA_EXPORT");
        }
      }
  }
}
