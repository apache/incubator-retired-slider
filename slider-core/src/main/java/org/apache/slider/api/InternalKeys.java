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
 * Keys for internal use, go into `internal.json` and not intended for normal
 * use except when tuning Slider AM operations
 */
public interface InternalKeys {


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
   * Time in milliseconds to wait after forking any in-AM 
   * process before attempting to start up the containers: {@value}
   *
   * A shorter value brings the cluster up faster, but means that if the
   * in AM process fails (due to a bad configuration), then time
   * is wasted starting containers on a cluster that isn't going to come
   * up
   */
  int DEFAULT_INTERNAL_CONTAINER_STARTUP_DELAY = 5000;
  /**
   * Version of the app: {@value}
   */
  String KEYTAB_LOCATION = "internal.keytab.location";


  /**
   * Flag to indicate whether or not the chaos monkey is enabled:
   * {@value}
   */
  String INTERNAL_CHAOS_MONKEY_ENABLED = "internal.chaos.monkey.enabled";
  boolean DEFAULT_INTERNAL_CHAOS_MONKEY_ENABLED = false;


  /**
   * Rate
   */

  String INTERNAL_CHAOS_MONKEY_RATE = "internal.chaos.monkey.rate";

  int DEFAULT_INTERNAL_CHAOS_MONKEY_RATE_DAYS = 0;
  int DEFAULT_INTERNAL_CHAOS_MONKEY_RATE_HOURS = 1;
  int DEFAULT_INTERNAL_CHAOS_MONKEY_RATE_MINUTES = 0;
  
  String INTERNAL_CHAOS_MONKEY_PROBABILITY =
      "internal.chaos.monkey.probability";
  /**
   * Probabilies are out of 10000 ; 100==1%
   */

  String INTERNAL_CHAOS_MONKEY_PROBABILITY_AM_FAILURE = INTERNAL_CHAOS_MONKEY_PROBABILITY +".amfailure";
  int DEFAULT_CHAOS_MONKEY_PROBABILITY_AM_FAILURE = 10;
  String INTERNAL_CHAOS_MONKEY_PROBABILITY_CONTAINER_FAILURE =
      INTERNAL_CHAOS_MONKEY_PROBABILITY + ".containerfailure";
  int DEFAULT_CHAOS_MONKEY_PROBABILITY_CONTAINER_FAILURE = 100;


}
