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

package org.apache.slider.common;

import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * These are the keys that can be added to <code>conf/slider-client.xml</code>.
 */
public interface SliderXmlConfKeys {
  String PREFIX_PROVIDER = "slider.provider";
  /**
   * pattern to identify a provider
   * {@value}
   */
  String KEY_PROVIDER = PREFIX_PROVIDER + ".%s";

  /**
   * HBase provider key (derived from {@link #KEY_PROVIDER}
   * and so not found in the code itself
   * {@value}
   */
  String KEY_PROVIDER_HBASE = PREFIX_PROVIDER + ".hbase";

  /**
   * Accumulo provider key (derived from {@link #KEY_PROVIDER}
   * and so not found in the code itself
   * {@value}
   */
  String KEY_PROVIDER_ACCUMULO =
    PREFIX_PROVIDER + ".accumulo";

  /**
   * Accumulo agent key (derived from {@link #KEY_PROVIDER}
   * and so not found in the code itself
   * {@value}
   */
  String KEY_PROVIDER_AGENT = PREFIX_PROVIDER + ".agent";

  /**
   * conf option set to point to where the config came from
   * {@value}
   */
  String KEY_TEMPLATE_ORIGIN = "slider.template.origin";

  /**
   * Original name for the default FS. This is still 
   * expected by applications deployed
   */
  String FS_DEFAULT_NAME_CLASSIC = "fs.default.name";

  /**
   * Slider principal
   */
  String KEY_KERBEROS_PRINCIPAL = "slider.kerberos.principal";

  /**
   * Name of the property for ACLs for Slider AM.
   * {@value}
   */
  String KEY_PROTOCOL_ACL = "security.slider.protocol.acl";

  /**
   * Limit on restarts for the AM
   * {@value}
   */
  String KEY_AM_RESTART_LIMIT = "slider.yarn.restart.limit";

  /**
   * Default Limit on restarts for the AM
   * {@value}
   */
  int DEFAULT_AM_RESTART_LIMIT = 2;

  /**
   * Flag which is set to indicate that security should be enabled
   * when talking to this cluster.
   */
  String KEY_SECURITY_ENABLED =
      CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION;

  /**
   * queue name
   */
  String KEY_YARN_QUEUE = "slider.yarn.queue";
  String DEFAULT_YARN_QUEUE = YarnConfiguration.DEFAULT_QUEUE_NAME;

  /**
   * default priority
   */
  String KEY_YARN_QUEUE_PRIORITY = "slider.yarn.queue.priority";
  int DEFAULT_YARN_QUEUE_PRIORITY = 1;


  /**
   * The slider base path: {@value}
   * Defaults to HomeDir/.slider
   */
  String KEY_SLIDER_BASE_PATH = "slider.base.path";


  /**
   * Option for the permissions for the cluster directory itself: {@value}
   */
  String CLUSTER_DIRECTORY_PERMISSIONS =
    "slider.cluster.directory.permissions";
  /**
   * Default value for the permissions :{@value}
   */
  String DEFAULT_CLUSTER_DIRECTORY_PERMISSIONS = "750";
  /**: {@value}
   * Option for the permissions for the data directory itself
   */
  String DATA_DIRECTORY_PERMISSIONS = "slider.data.directory.permissions";
  /**
   * Default value for the data directory permissions: {@value}
   */
  String DEFAULT_DATA_DIRECTORY_PERMISSIONS = "750";


  String REGISTRY_PATH = "slider.registry.path";

  /**
   * Default value for the registry: {@value}
   */
  String DEFAULT_REGISTRY_PATH = "/registry";


  String REGISTRY_ZK_QUORUM = "slider.zookeeper.quorum";

  /**
   * Default value for the registry: {@value}
   */
  String DEFAULT_REGISTRY_ZK_QUORUM = "localhost:2181";


  String IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH =
      "ipc.client.fallback-to-simple-auth-allowed";
}
