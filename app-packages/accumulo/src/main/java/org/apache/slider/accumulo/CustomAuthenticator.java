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
package org.apache.slider.accumulo;

import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.security.thrift.TCredentials;
import org.apache.accumulo.server.security.handler.Authenticator;
import org.apache.accumulo.server.security.handler.Authorizor;
import org.apache.accumulo.server.security.handler.PermissionHandler;
import org.apache.accumulo.server.security.handler.ZKAuthenticator;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Set;

public final class CustomAuthenticator implements Authenticator {
  public static final String ROOT_INITIAL_PASSWORD_PROPERTY =
      "root.initial.password";
  private static ZKAuthenticator zkAuthenticator = null;

  public CustomAuthenticator() {
    zkAuthenticator = new ZKAuthenticator();
  }

  @Override
  public void initialize(String instanceId, boolean initialize) {
    zkAuthenticator.initialize(instanceId, initialize);
  }

  @Override
  public void initializeSecurity(TCredentials credentials, String principal,
      byte[] token) throws AccumuloSecurityException {
    String pass = null;
    SiteConfiguration siteconf = SiteConfiguration.getInstance(
        DefaultConfiguration.getInstance());
    String jksFile = siteconf.get(
        Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS);

    if (jksFile == null) {
      throw new RuntimeException(
          Property.GENERAL_SECURITY_CREDENTIAL_PROVIDER_PATHS +
              " not specified in accumulo-site.xml");
    }
    try {
      pass = new String(ProviderUtil.getPassword(jksFile,
          ROOT_INITIAL_PASSWORD_PROPERTY));
    } catch (IOException ioe) {
      throw new RuntimeException("Can't get key " +
          ROOT_INITIAL_PASSWORD_PROPERTY + " from " + jksFile, ioe);
    }
    zkAuthenticator.initializeSecurity(credentials, principal,
        pass.getBytes(Charset.forName("UTF-8")));
  }

  @Override
  public Set<String> listUsers() {
    return zkAuthenticator.listUsers();
  }

  @Override
  public void createUser(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    zkAuthenticator.createUser(principal, token);
  }

  @Override
  public void dropUser(String user) throws AccumuloSecurityException {
    zkAuthenticator.dropUser(user);
  }

  @Override
  public void changePassword(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    zkAuthenticator.changePassword(principal, token);
  }

  @Override
  public boolean userExists(String user) {
    return zkAuthenticator.userExists(user);
  }

  @Override
  public boolean validSecurityHandlers(Authorizor auth, PermissionHandler pm) {
    return true;
  }

  @Override
  public boolean authenticateUser(String principal, AuthenticationToken token) throws AccumuloSecurityException {
    return zkAuthenticator.authenticateUser(principal, token);
  }

  @Override
  public Set<Class<? extends AuthenticationToken>> getSupportedTokenTypes() {
    return zkAuthenticator.getSupportedTokenTypes();
  }

  @Override
  public boolean validTokenClass(String tokenClass) {
    return zkAuthenticator.validTokenClass(tokenClass);
  }
}
