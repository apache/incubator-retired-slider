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
package org.apache.slider.funtest.accumulo;


import org.apache.slider.accumulo.ProviderUtil;
import sun.security.x509.AlgorithmId;
import sun.security.x509.CertificateAlgorithmId;
import sun.security.x509.CertificateIssuerName;
import sun.security.x509.CertificateSerialNumber;
import sun.security.x509.CertificateSubjectName;
import sun.security.x509.CertificateValidity;
import sun.security.x509.CertificateVersion;
import sun.security.x509.CertificateX509Key;
import sun.security.x509.X500Name;
import sun.security.x509.X509CertImpl;
import sun.security.x509.X509CertInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Enumeration;

public class CertUtil {

  public static void createRootKeyPair(String keyStoreFile,
      String keyStorePasswordProperty, String trustStoreFile,
      String trustStorePasswordProperty, String credentialProvider)
      throws Exception {
    char[] keyStorePassword = ProviderUtil.getPassword(credentialProvider,
        keyStorePasswordProperty);
    char[] trustStorePassword = ProviderUtil.getPassword(credentialProvider,
        trustStorePasswordProperty);

    createSelfSignedCert(keyStoreFile, "root", keyStorePassword);
    createPublicCert(trustStoreFile, "root", keyStoreFile, keyStorePassword,
        trustStorePassword);
  }

  public static void createServerKeyPair(String keyStoreFile,
      String keyStorePasswordProperty, String rootKeyStoreFile,
      String rootKeyStorePasswordProperty, String credentialProvider,
      String hostname)
      throws Exception {
    char[] keyStorePassword = ProviderUtil.getPassword(credentialProvider,
        keyStorePasswordProperty);
    char[] rootKeyStorePassword = ProviderUtil.getPassword(credentialProvider,
        rootKeyStorePasswordProperty);

    createSignedCert(keyStoreFile, "server", hostname, keyStorePassword,
        rootKeyStoreFile, rootKeyStorePassword);
  }


  private static final String keystoreType = "JKS";
  private static final int keysize = 2048;
  private static final String encryptionAlgorithm = "RSA";
  private static final String signingAlgorithm = "SHA256WITHRSA";
  private static final String issuerDirString = ",O=Apache Slider";

  public static void createPublicCert(String targetKeystoreFile, String keyName,
      String rootKeystorePath, char[] rootKeystorePassword,
      char[] truststorePassword) throws KeyStoreException,
      IOException, CertificateException, NoSuchAlgorithmException {
    KeyStore signerKeystore = KeyStore.getInstance(keystoreType);
    char[] signerPasswordArray = rootKeystorePassword;
    signerKeystore.load(new FileInputStream(rootKeystorePath), signerPasswordArray);
    Certificate rootCert = findCert(signerKeystore);

    KeyStore keystore = KeyStore.getInstance(keystoreType);
    keystore.load(null, null);
    keystore.setCertificateEntry(keyName + "Cert", rootCert);
    keystore.store(new FileOutputStream(targetKeystoreFile), truststorePassword);
  }

  public static void createSignedCert(String targetKeystoreFile,
      String keyName, String hostname, char[] keystorePassword,
      String signerKeystorePath, char[] signerKeystorePassword)
      throws Exception {
    KeyStore signerKeystore = KeyStore.getInstance(keystoreType);
    char[] signerPasswordArray = signerKeystorePassword;
    signerKeystore.load(new FileInputStream(signerKeystorePath), signerPasswordArray);
    Certificate signerCert = findCert(signerKeystore);
    PrivateKey signerKey = findPrivateKey(signerKeystore, signerPasswordArray);

    KeyPair kp = generateKeyPair();
    Certificate cert = generateCert(hostname, kp, false,
        signerCert.getPublicKey(), signerKey);

    char[] password = keystorePassword;
    KeyStore keystore = KeyStore.getInstance(keystoreType);
    keystore.load(null, null);
    keystore.setCertificateEntry(keyName + "Cert", cert);
    keystore.setKeyEntry(keyName + "Key", kp.getPrivate(), password, new Certificate[] {cert, signerCert});
    keystore.store(new FileOutputStream(targetKeystoreFile), password);
  }

  public static void createSelfSignedCert(String targetKeystoreFileName,
      String keyName, char[] keystorePassword)
      throws IOException, NoSuchAlgorithmException, CertificateException,
      NoSuchProviderException, InvalidKeyException, SignatureException,
      KeyStoreException {
    File targetKeystoreFile = new File(targetKeystoreFileName);
    if (targetKeystoreFile.exists()) {
      throw new IOException("File exists: "+targetKeystoreFile);
    }

    KeyPair kp = generateKeyPair();

    Certificate cert = generateCert(null, kp, true,
        kp.getPublic(), kp.getPrivate());

    char[] password = keystorePassword;
    KeyStore keystore = KeyStore.getInstance(keystoreType);
    keystore.load(null, null);
    keystore.setCertificateEntry(keyName + "Cert", cert);
    keystore.setKeyEntry(keyName + "Key", kp.getPrivate(), password, new Certificate[] {cert});
    keystore.store(new FileOutputStream(targetKeystoreFile), password);
  }

  private static KeyPair generateKeyPair() throws NoSuchAlgorithmException {
    KeyPairGenerator gen = KeyPairGenerator.getInstance(encryptionAlgorithm);
    gen.initialize(keysize);
    return gen.generateKeyPair();
  }

  private static X509Certificate generateCert(
      String hostname, KeyPair kp, boolean isCertAuthority,
      PublicKey signerPublicKey, PrivateKey signerPrivateKey)
      throws IOException, CertificateException, NoSuchProviderException,
      NoSuchAlgorithmException, InvalidKeyException, SignatureException {
    X500Name issuer = new X500Name("CN=root" + issuerDirString);
    X500Name subject;
    if (hostname == null) {
      subject = issuer;
    } else {
      subject = new X500Name("CN=" + hostname + issuerDirString);
    }

    X509CertInfo info = new X509CertInfo();
    Date from = new Date();
    Date to = new Date(from.getTime() + 365 * 86400000l);
    CertificateValidity interval = new CertificateValidity(from, to);
    BigInteger sn = new BigInteger(64, new SecureRandom());

    info.set(X509CertInfo.VALIDITY, interval);
    info.set(X509CertInfo.SERIAL_NUMBER, new CertificateSerialNumber(sn));
    info.set(X509CertInfo.SUBJECT, new CertificateSubjectName(subject));
    info.set(X509CertInfo.ISSUER, new CertificateIssuerName(issuer));
    info.set(X509CertInfo.KEY, new CertificateX509Key(kp.getPublic()));
    info.set(X509CertInfo.VERSION, new CertificateVersion(CertificateVersion.V3));
    AlgorithmId algo = new AlgorithmId(AlgorithmId.md5WithRSAEncryption_oid);
    info.set(X509CertInfo.ALGORITHM_ID, new CertificateAlgorithmId(algo));

    // Sign the cert to identify the algorithm that's used.
    X509CertImpl cert = new X509CertImpl(info);
    cert.sign(signerPrivateKey, signingAlgorithm);

    // Update the algorithm, and resign.
    algo = (AlgorithmId)cert.get(X509CertImpl.SIG_ALG);
    info.set(CertificateAlgorithmId.NAME + "." + CertificateAlgorithmId.ALGORITHM, algo);
    cert = new X509CertImpl(info);
    cert.sign(signerPrivateKey, signingAlgorithm);
    return cert;
  }

  private static Certificate findCert(KeyStore keyStore) throws KeyStoreException {
    Enumeration<String> aliases = keyStore.aliases();
    Certificate cert = null;
    while (aliases.hasMoreElements()) {
      String alias = aliases.nextElement();
      if (keyStore.isCertificateEntry(alias)) {
        // assume only one cert
        cert = keyStore.getCertificate(alias);
        break;
      }
    }
    if (cert == null) {
      throw new KeyStoreException("Could not find cert in keystore");
    }
    return cert;
  }

  private static PrivateKey findPrivateKey(KeyStore keyStore, char[] keystorePassword)
      throws KeyStoreException, UnrecoverableKeyException, NoSuchAlgorithmException {
    Enumeration<String> aliases = keyStore.aliases();
    PrivateKey key = null;
    while (aliases.hasMoreElements()) {
      String alias = aliases.nextElement();
      if (keyStore.isKeyEntry(alias)) {
        // assume only one key
        key = (PrivateKey) keyStore.getKey(alias, keystorePassword);
        break;
      }
    }
    if (key == null) {
      throw new KeyStoreException("Could not find private key in keystore");
    }
    return key;
  }

}
