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
package org.apache.slider.server.services.security;

import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.core.conf.MapOperations;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Enumeration;

/**
 *
 */
public class TestCertificateManager {
  @Rule
  public TemporaryFolder workDir = new TemporaryFolder();
  private File secDir;
  private CertificateManager certMan;

  @Before
  public void setup() throws Exception {
    certMan = new CertificateManager();
    MapOperations compOperations = new MapOperations();
    secDir = new File(workDir.getRoot(), SliderKeys.SECURITY_DIR);
    File keystoreFile = new File(secDir, SliderKeys.KEYSTORE_FILE_NAME);
    compOperations.put(SliderXmlConfKeys.KEY_KEYSTORE_LOCATION,
                       keystoreFile.getAbsolutePath());
    certMan.initialize(compOperations);
  }

  @Test
  public void testServerCertificateGenerated() throws Exception {
    File serverCrt = new File(secDir, SliderKeys.CRT_FILE_NAME);
    Assert.assertTrue("Server CRD does not exist:" + serverCrt,
                      serverCrt.exists());
  }

  @Test
  public void testAMKeystoreGenerated() throws Exception {
    File keystoreFile = new File(secDir, SliderKeys.KEYSTORE_FILE_NAME);
    Assert.assertTrue("Keystore does not exist: " + keystoreFile,
                      keystoreFile.exists());
    InputStream is = null;
    try {

      is = new FileInputStream(keystoreFile);
      KeyStore keystore = KeyStore.getInstance("pkcs12");
      String password = SecurityUtils.getKeystorePass();
      keystore.load(is, password.toCharArray());

      Certificate certificate = keystore.getCertificate(
          keystore.aliases().nextElement());
      Assert.assertNotNull(certificate);

      if (certificate instanceof X509Certificate) {
        X509Certificate x509cert = (X509Certificate) certificate;

        // Get subject
        Principal principal = x509cert.getSubjectDN();
        String subjectDn = principal.getName();
        Assert.assertEquals("wrong DN",
                            "O=Default Company Ltd, L=Default City, ST=Default Province, C=XX",
                            subjectDn);

        // Get issuer
        principal = x509cert.getIssuerDN();
        String issuerDn = principal.getName();
        Assert.assertEquals("wrong Issuer DN",
                            "O=Default Company Ltd, L=Default City, ST=Default Province, C=XX",
                            issuerDn);
      }
    } finally {
      if(null != is) {
        is.close();
      }
    }
  }

  @Test
  public void testContainerCertificateGeneration() throws Exception {
    certMan.generateContainerCertificate("localhost", "container1");
    Assert.assertTrue("container certificate not generated",
                      new File(secDir, "container1.crt").exists());
  }

  @Test
  public void testContainerKeystoreGeneration() throws Exception {
    certMan.generateContainerKeystore("localhost", "container1", "password");
    File keystoreFile = new File(secDir, "localhost-container1.p12");
    Assert.assertTrue("container keystore not generated",
                      keystoreFile.exists());

    InputStream is = null;
    try {

      is = new FileInputStream(keystoreFile);
      KeyStore keystore = KeyStore.getInstance("pkcs12");
      String password = "password";
      keystore.load(is, password.toCharArray());

      Certificate certificate = keystore.getCertificate(
          keystore.aliases().nextElement());
      Assert.assertNotNull(certificate);

      if (certificate instanceof X509Certificate) {
        X509Certificate x509cert = (X509Certificate) certificate;

        // Get subject
        Principal principal = x509cert.getSubjectDN();
        String subjectDn = principal.getName();
        Assert.assertEquals("wrong DN", "CN=container1, OU=localhost",
                            subjectDn);

        // Get issuer
        principal = x509cert.getIssuerDN();
        String issuerDn = principal.getName();
        Assert.assertEquals("wrong Issuer DN",
                            "O=Default Company Ltd, L=Default City, ST=Default Province, C=XX",
                            issuerDn);
      }
    } finally {
      if(null != is) {
        is.close();
      }
    }
  }

}
