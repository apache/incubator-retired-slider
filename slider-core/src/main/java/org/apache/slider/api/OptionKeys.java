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

package org.apache.slider.api;

/**
 *  Keys for entries in the <code>options</code> section
 *  of a cluster description.
 */
public interface OptionKeys {

  /**
   * Home dir of the app: {@value}
   * If set, implies there is a home dir to use
   */
  String INTERNAL_APPLICATION_HOME = "internal.application.home";
  
  /**
   * Path to an image file containing the app: {@value}
   */
  String INTERNAL_APPLICATION_IMAGE_PATH = "internal.application.image.path";

  /**
   * Time in milliseconds to wait after forking any in-AM 
   * process before attempting to start up the containers: {@value}
   * 
   * A shorter value brings the cluster up faster, but means that if the
   * in AM process fails (due to a bad configuration), then time
   * is wasted starting containers on a cluster that isn't going to come
   * up
   */
  String INTERNAL_CONTAINER_STARTUP_DELAY = "internal.container.startup.delay";
  
  /**
   * Time in milliseconds to wait after forking any in-AM 
   * process before attempting to start up the containers: {@value}
   * 
   * A shorter value brings the cluster up faster, but means that if the
   * in AM process fails (due to a bad configuration), then time
   * is wasted starting containers on a cluster that isn't going to come
   * up
   */
  String APPLICATION_TYPE = "application.type";
  
  /**
   * Time in milliseconds to wait after forking any in-AM 
   * process before attempting to start up the containers: {@value}
   * 
   * A shorter value brings the cluster up faster, but means that if the
   * in AM process fails (due to a bad configuration), then time
   * is wasted starting containers on a cluster that isn't going to come
   * up
   */
  String APPLICATION_NAME = "application.name";

  /**
   * Time in milliseconds before a container is considered long-lived.
   * Shortlived containers are interpreted as a problem with the role
   * and/or the host: {@value}
   */
  String INTERNAL_CONTAINER_FAILURE_SHORTLIFE = "internal.container.failure.shortlife";

  /**
   * Default short life threshold: {@value}
   */
  int DEFAULT_CONTAINER_FAILURE_SHORTLIFE = 60;

  /**
   * maximum number of failed containers (in a single role)
   * before the cluster is deemed to have failed {@value}
   */
  String INTERNAL_CONTAINER_FAILURE_THRESHOLD = "internal.container.failure.threshold";

  /**
   * Default failure threshold: {@value}
   */
  int DEFAULT_CONTAINER_FAILURE_THRESHOLD = 5;

  /**
   * delay for container startup:{@value}
   */
  int DEFAULT_CONTAINER_STARTUP_DELAY = 5000;

  /**
   * Version of the app: {@value}
   */
  String KEYTAB_LOCATION = "internal.keytab.location";

  /**
   * Prefix for site.xml options: {@value}
   */
  String SITE_XML_PREFIX = "site.";

  /**
   * internal temp directory: {@value}
   */
  String INTERNAL_AM_TMP_DIR = "internal.tmp.dir";

  /**
   * where a snapshot of the original conf dir is: {@value}
   */
  String INTERNAL_SNAPSHOT_CONF_PATH = "internal.snapshot.conf.path";
  
  /**
   * where a snapshot of the original conf dir is: {@value}
   */
  String INTERNAL_GENERATED_CONF_PATH = "internal.generated.conf.path";
    
  /**
   * where a snapshot of the original conf dir is: {@value}
   */
  String INTERNAL_PROVIDER_NAME = "internal.provider.name";
  
    
  /**
   * where a snapshot of the original conf dir is: {@value}
   */
  String INTERNAL_DATA_DIR_PATH = "internal.data.dir.path";
  
  /**
   * Zookeeper quorum host list: {@value}
   */
  String ZOOKEEPER_QUORUM = "zookeeper.quorum";
  String ZOOKEEPER_HOSTS = "zookeeper.hosts";
  String ZOOKEEPER_PORT = "zookeeper.port";

  /**
   * Zookeeper path value (string): {@value}
   */
  String ZOOKEEPER_PATH = "zookeeper.path";

}
