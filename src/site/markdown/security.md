<!---
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

# Security

This document discusses the design, implementation and use of Slider
to deploy secure applications on a secure Hadoop cluster.

### Important:
 
This document does not cover Kerberos, how to secure a Hadoop cluster, Kerberos
command line tools or how Hadoop uses delegation tokens to delegate permissions
round a cluster. These are assumed, though some links to useful pages are
listed at the bottom. 


## Concepts

Slider runs in secure clusters, but with restrictions

1. The keytabs to allow a worker to authenticate with the master must
   be distributed in advance: Slider does not attempt to pass these around.
1. Until the location of Slider node instances can be strictly limited to
  a set of nodes (a future YARN feature), the keytabs must be passed to
  all the nodes in the cluster in advance, *and made available to the
  user creating the cluster*
1. due to the way that HBase and accumulo authenticate worker nodes to
  the masters, any HBase node running on a server must authenticate as
  the same principal, and so have equal access rights to the HBase cluster.
1. As the data directories for a slider cluster are created under the home
  directories of that user, the principals representing all role instances
  in the clusters *MUST* have read/write access to these files. This can be
  done with a shortname that matches that of the user, or by requesting
  that Slider create a directory with group write permissions -and using LDAP
  to indentify the application principals as members of the same group
  as the user.


## Requirements


### Needs
*  Slider and HBase to work against secure HDFS
*  Slider to work with secure YARN.
*  Slider to start a secure HBase cluster
*  Kerberos and ActiveDirectory to perform the authentication.
*  Slider to only allow cluster operations by authenticated users -command line and direct RPC. 
*  Any Slider Web UI and REST API for Ambari to only allow access to authenticated users.
*  The Slider database in ~/.hoya/clusters/$name/data to be writable by HBase


### Short-lived Clusters
*  Cluster to remain secure for the duration of the Kerberos tokens issued to Slider.


### Long-lived Clusters

*  Slider application instance and HBase instance to remain functional and secure over an indefinite period of time.

### Initial Non-requirements
*  secure audit trail of cluster operations.
*  multiple authorized users being granted rights to a Slider Cluster (YARN admins can always kill the Slider application instance.
*  More than one HBase cluster in the YARN cluster belonging to a single user (irrespective of how they are started).
*  Any way to revoke certificates/rights of running containers.

### Assumptions
*  Kerberos is running and that HDFS and YARN are running Kerberized.
*  LDAP cannot be assumed. 
*  Credentials needed for HBase can be pushed out into the local filesystems of 
  the of the worker nodes via some external mechanism (e.g. scp), and protected by
  the access permissions of the native filesystem. Any user with access to these
  credentials is considered to have been granted such rights.
*  These credentials can  outlive the duration of the HBase containers
*  The user running HBase has the same identity as that of the HBase cluster.

## Design


1. The user is expected to have their own Kerberos principal, and have used `kinit`
  or equivalent to authenticate with Kerberos and gain a (time-bounded) TGT
1. The user is expected to have their own principals for every host in the cluster of the form
  username/hostname@REALM
1. A keytab must be generated which contains all these principals -and distributed
  to all the nodes in the cluster with read access permissions to the user.
1. When the user creates a secure cluster, they provide the standard HBase kerberos options
  to identify the principals to use and the keytab location.

The Slider Client will talk to HDFS and YARN authenticating itself with the TGT,
talking to the YARN and HDFS principals which it has been configured to expect.

This can be done as described in [Client Configuration] (client-configuration.html) on the command line as

     -D yarn.resourcemanager.principal=yarn/master@LOCAL 
     -D dfs.namenode.kerberos.principal=hdfs/master@LOCAL

The Slider Client will create the cluster data directory in HDFS with `rwx` permissions for  
user `r-x` for the group and `---` for others. (these can be configurable as part of the cluster options), 

It will then deploy the AM, which will (somehow? for how long?) retain the access
rights of the user that created the cluster.

The Application Master will read in the JSON cluster specification file, and instantiate the
relevant number of componentss. 


## Securing communications between the Slider Client and the Slider AM.

When the AM is deployed in a secure cluster,
it automatically uses Kerberos-authorized RPC channels. The client must acquire a
token to talk the AM. 

This is provided by the YARN Resource Manager when the client application
wishes to talk with the HoyaAM -a token which is only provided after
the caller authenticates itself as the user that has access rights
to the cluster

To allow the client to freeze a Slider application instance while they are unable to acquire
a token to authenticate with the AM, use the `--force` option.

### How to enable a secure Slider client

Slider can be placed into secure mode by setting the Hadoop security options:

This can be done in `slider-client.xml`:


  <property>
    <name>hadoop.security.authorization</name>
    <value>true</value>
  </property>

  <property>
    <name>hadoop.security.authentication</name>
    <value>kerberos</value>
  </property>


Or it can be done on the command line

    -D hadoop.security.authorization=true -D hadoop.security.authentication=kerberos

### Adding Kerberos binding properties to the Slider Client JVM

The Java Kerberos library needs to know the Kerberos controller and
realm to use. This should happen automatically if this is set up as the
default Kerberos binding (on a Unix system this is done in `/etc/krb5.conf`.

If is not set up, a stack trace with kerberos classes at the top and
the message `java.lang.IllegalArgumentException: Can't get Kerberos realm`
will be printed -and the client will then fail.

The realm and controller can be defined in the Java system properties
`java.security.krb5.realm` and `java.security.krb5.kdc`. These can be fixed
in the JVM options, as described in the [Client Configuration] (hoya-client-configuration.html)
documentation.

They can also be set on the Slider command line itself, using the `-S` parameter.

    -S java.security.krb5.realm=MINICLUSTER  -S java.security.krb5.kdc=hadoop-kdc

### Java Cryptography Exceptions 


When trying to talk to a secure, cluster you may see the message:

    No valid credentials provided (Mechanism level: Illegal key size)]

This means that the JRE does not have the extended cryptography package
needed to work with the keys that Kerberos needs. This must be downloaded
from Oracle (or other supplier of the JVM) and installed according to
its accompanying instructions.

## Useful Links

1. [Adding Security to Apache Hadoop](http://hortonworks.com/wp-content/uploads/2011/10/security-design_withCover-1.pdf)
1. [The Role of Delegation Tokens in Apache Hadoop Security](http://hortonworks.com/blog/the-role-of-delegation-tokens-in-apache-hadoop-security/)
1. [Chapter 8. Secure Apache HBase](http://hbase.apache.org/book/security.html)
1. Hadoop Operations p135+
1. [Java Kerberos Requirements](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/KerberosReq.htmla)
1. [Troubleshooting Kerberos on Java](http://docs.oracle.com/javase/7/docs/technotes/guides/security/jgss/tutorials/Troubleshooting.html)
1. For OS/X users, the GUI ticket viewer is `/System/Library/CoreServices/Ticket\ Viewer.app`


