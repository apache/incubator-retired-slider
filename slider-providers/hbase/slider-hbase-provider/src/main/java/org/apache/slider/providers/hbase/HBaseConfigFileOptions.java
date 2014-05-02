/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.slider.providers.hbase;

/**
 * Mappings of config params to env variables for
 * custom -site.xml files to pick up
 *
 * A lot of these come from HConstants -the reason they have been copied
 * and pasted in here is to remove dependencies on HBase from
 * the Slider Client and AM.
 */
public interface HBaseConfigFileOptions {

  String KEY_HBASE_CLUSTER_DISTRIBUTED = "hbase.cluster.distributed";
   String KEY_HBASE_ROOTDIR = "hbase.rootdir";

  String KEY_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
  //HConstants.ZOOKEEPER_QUORUM;
  String KEY_ZOOKEEPER_PORT = "hbase.zookeeper.property.clientPort";
  //HConstants.ZOOKEEPER_CLIENT_PORT;
  String KEY_ZNODE_PARENT = "zookeeper.znode.parent";


  int DEFAULT_MASTER_PORT = 60000;
  int DEFAULT_MASTER_INFO_PORT = 60010;

  String KEY_HBASE_MASTER_PORT = "hbase.master.port";
  String KEY_HBASE_MASTER_INFO_PORT = "hbase.master.info.port";

  int HBASE_ZK_PORT = 2181; // HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;


  String KEY_REGIONSERVER_PORT = "hbase.regionserver.port";
  String KEY_REGIONSERVER_INFO_PORT = "hbase.regionserver.info.port";

  /**
   * needed to relax constraints in negotiations, including tests
   */
  String IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH =
    "ipc.client.fallback-to-simple-auth-allowed";

  /*
      <property>
        <name>hbase.regionserver.kerberos.principal</name>
        <value>hbase/_HOST@YOUR-REALM.COM</value>
      </property>
      <property>
        <name>hbase.regionserver.keytab.file</name>
        <value>/etc/hbase/conf/keytab.krb5</value>
      </property>
      <property>
        <name>hbase.master.kerberos.principal</name>
        <value>hbase/_HOST@YOUR-REALM.COM</value>
      </property>
      <property>
        <name>hbase.master.keytab.file</name>
        <value>/etc/hbase/conf/keytab.krb5</value>
      </property>
   */


  String KEY_REGIONSERVER_KERBEROS_PRINCIPAL = "hbase.regionserver.kerberos.principal";
  String KEY_REGIONSERVER_KERBEROS_KEYTAB = "hbase.regionserver.keytab.file";
  
  String KEY_MASTER_KERBEROS_PRINCIPAL = "hbase.master.kerberos.principal";
  String KEY_MASTER_KERBEROS_KEYTAB = "hbase.master.keytab.file";
  

}
