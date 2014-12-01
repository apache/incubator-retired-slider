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
package org.apache.slider.server.appmaster.security

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.CommonConfigurationKeysPublic
import org.apache.hadoop.security.UserGroupInformation
import org.apache.slider.common.SliderKeys
import org.apache.slider.common.SliderXmlConfKeys
import org.apache.slider.core.conf.AggregateConf
import org.apache.slider.core.conf.MapOperations
import org.apache.slider.core.exceptions.SliderException;
import org.junit.Test;

/**
 *
 */
public class SecurityConfigurationTest {
  final shouldFail = new GroovyTestCase().&shouldFail

  @Test
  public void testValidLocalConfiguration() throws Throwable {
      Configuration config = new Configuration()
      config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")
      AggregateConf aggregateConf = new AggregateConf();
      MapOperations compOps =
          aggregateConf.appConfOperations.getOrAddComponent(SliderKeys.COMPONENT_AM)
      compOps.put(SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL, "test")
      compOps.put(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH, "/some/local/path")

      SecurityConfiguration securityConfiguration =
          new SecurityConfiguration(config, aggregateConf, "testCluster")
  }

    @Test
    public void testValidDistributedConfiguration() throws Throwable {
        Configuration config = new Configuration()
        config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")
        AggregateConf aggregateConf = new AggregateConf();
        MapOperations compOps =
            aggregateConf.appConfOperations.getOrAddComponent(SliderKeys.COMPONENT_AM)
        compOps.put(SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL, "test")
        compOps.put(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME, "some.keytab")

        SecurityConfiguration securityConfiguration =
            new SecurityConfiguration(config, aggregateConf, "testCluster")
    }

    @Test
    public void testMissingPrincipalNoLoginWithDistributedConfig() throws Throwable {
        Configuration config = new Configuration()
        config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")
        AggregateConf aggregateConf = new AggregateConf();
        MapOperations compOps =
            aggregateConf.appConfOperations.getOrAddComponent(SliderKeys.COMPONENT_AM)
        compOps.put(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME, "some.keytab")

        shouldFail(SliderException) {
            SecurityConfiguration securityConfiguration =
                new SecurityConfiguration(config, aggregateConf, "testCluster") {
                    @Override
                    protected UserGroupInformation getLoginUser() throws IOException {
                        return null
                    }
                }
        }
    }

    @Test
    public void testMissingPrincipalNoLoginWithLocalConfig() throws Throwable {
        Configuration config = new Configuration()
        config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")
        AggregateConf aggregateConf = new AggregateConf();
        MapOperations compOps =
            aggregateConf.appConfOperations.getOrAddComponent(SliderKeys.COMPONENT_AM)
        compOps.put(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH, "/some/local/path")

        shouldFail(SliderException) {
            SecurityConfiguration securityConfiguration =
                new SecurityConfiguration(config, aggregateConf, "testCluster") {
                    @Override
                    protected UserGroupInformation getLoginUser() throws IOException {
                        return null
                    }
                }
        }
    }

    @Test
    public void testBothKeytabMechanismsConfigured() throws Throwable {
        Configuration config = new Configuration()
        config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")
        AggregateConf aggregateConf = new AggregateConf();
        MapOperations compOps =
            aggregateConf.appConfOperations.getOrAddComponent(SliderKeys.COMPONENT_AM)
        compOps.put(SliderXmlConfKeys.KEY_KEYTAB_PRINCIPAL, "test")
        compOps.put(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH, "/some/local/path")
        compOps.put(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME, "some.keytab")

        shouldFail(SliderException) {
            SecurityConfiguration securityConfiguration =
                new SecurityConfiguration(config, aggregateConf, "testCluster")
        }
    }

    @Test
    public void testMissingPrincipalButLoginWithDistributedConfig() throws Throwable {
        Configuration config = new Configuration()
        config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")
        AggregateConf aggregateConf = new AggregateConf();
        MapOperations compOps =
            aggregateConf.appConfOperations.getOrAddComponent(SliderKeys.COMPONENT_AM)
        compOps.put(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME, "some.keytab")

        SecurityConfiguration securityConfiguration =
            new SecurityConfiguration(config, aggregateConf, "testCluster")
    }

    @Test
    public void testMissingPrincipalButLoginWithLocalConfig() throws Throwable {
        Configuration config = new Configuration()
        config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")
        AggregateConf aggregateConf = new AggregateConf();
        MapOperations compOps =
            aggregateConf.appConfOperations.getOrAddComponent(SliderKeys.COMPONENT_AM)
        compOps.put(SliderXmlConfKeys.KEY_AM_KEYTAB_LOCAL_PATH, "/some/local/path")

        SecurityConfiguration securityConfiguration =
            new SecurityConfiguration(config, aggregateConf, "testCluster")
    }

    @Test
    public void testKeypathLocationOnceLocalized() throws Throwable {
        Configuration config = new Configuration()
        config.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos")
        AggregateConf aggregateConf = new AggregateConf();
        MapOperations compOps =
            aggregateConf.appConfOperations.getOrAddComponent(SliderKeys.COMPONENT_AM)
        compOps.put(SliderXmlConfKeys.KEY_AM_LOGIN_KEYTAB_NAME, "some.keytab")

        SecurityConfiguration securityConfiguration =
            new SecurityConfiguration(config, aggregateConf, "testCluster")

        assert new File(SliderKeys.KEYTAB_DIR, "some.keytab").getAbsolutePath() ==
               securityConfiguration.getKeytabFile(aggregateConf).getAbsolutePath()
    }

}
