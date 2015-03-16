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

import org.junit.BeforeClass

import javax.net.ssl.KeyManager
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import java.security.SecureRandom
import java.security.cert.CertificateException
import java.security.cert.X509Certificate

class AccumuloSSLTestBase extends AccumuloBasicIT {

  protected String templateName() {
    return sysprop("test.app.resources.dir") + "/appConfig_ssl.json"
  }

  protected String getDefaultTemplate() {
    return sysprop("test.app.resources.dir") + "/appConfig-ssl-default.json"
  }

  @Override
  public String getClusterName() {
    return "test_ssl";
  }

  @Override
  public String getDescription() {
    return "Test enable SSL $clusterName"
  }

  @BeforeClass
  public static void initHttps() {
    SSLContext ctx = SSLContext.getInstance("SSL");
    TrustManager[] t = new TrustManager[1];
    t[0] = new DefaultTrustManager();
    ctx.init(new KeyManager[0], t, new SecureRandom());
    SSLContext.setDefault(ctx);
  }

  private static class DefaultTrustManager implements X509TrustManager {
    @Override
    public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {}

    @Override
    public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {}

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return null;
    }
  }
}
