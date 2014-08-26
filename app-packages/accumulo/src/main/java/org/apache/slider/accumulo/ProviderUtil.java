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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProvider.CredentialEntry;
import org.apache.hadoop.security.alias.CredentialProviderFactory;

import java.io.IOException;
import java.util.List;

public class ProviderUtil {
  public static CredentialProvider getProvider(String credentialProvider)
      throws IOException {
    Configuration conf = new Configuration();
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        credentialProvider);
    List<CredentialProvider> providers =
        CredentialProviderFactory.getProviders(conf);

    if (providers == null || providers.size() != 1) {
      throw new IOException("Found unexpected number of providers");
    }
    return providers.get(0);
  }

  public static char[] getPassword(String credentialProvider, String alias)
      throws IOException {
    return getPassword(getProvider(credentialProvider), alias);
  }

  public static char[] getPassword(CredentialProvider provider, String alias)
      throws IOException {
    CredentialEntry entry = provider.getCredentialEntry(alias);
    if (entry != null) {
      return entry.getCredential();
    }
    throw new IOException("Can't get key " + alias + " from " +
        provider.toString());
  }
}
