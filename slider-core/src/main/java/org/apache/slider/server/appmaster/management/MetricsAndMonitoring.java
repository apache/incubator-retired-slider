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

package org.apache.slider.server.appmaster.management;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.service.CompositeService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Class for all metrics and monitoring
 */
public class MetricsAndMonitoring extends CompositeService {

  public MetricsAndMonitoring(String name) {
    super(name);
  }
  
  public MetricsAndMonitoring() {
    super("MetricsAndMonitoring");
  }
  
  /**
   * Singleton of metrics registry
   */
  final MetricRegistry metrics = new MetricRegistry();

  final HealthCheckRegistry health = new HealthCheckRegistry();

  private final Map<String, MeterAndCounter> meterAndCounterMap
      = new ConcurrentHashMap<String, MeterAndCounter>();
  
  
  public MetricRegistry getMetrics() {
    return metrics;
  }

  public HealthCheckRegistry getHealth() {
    return health;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    addService(new MetricsBindingService("MetricsBindingService",
        metrics));
    super.serviceInit(conf);
  }

  public MeterAndCounter getMeterAndCounter(String name) {
    return meterAndCounterMap.get(name);
  }

  /**
   * Get or create the meter/counter pair
   * @param name name of instance
   * @return an instance
   */
  public MeterAndCounter getOrCreateMeterAndCounter(String name) {
    MeterAndCounter instance = meterAndCounterMap.get(name);
    if (instance == null) {
      synchronized (this) {
        // check in a sync block
        instance = meterAndCounterMap.get(name);
        if (instance == null) {
          instance = new MeterAndCounter(metrics, name);
          meterAndCounterMap.put(name, instance);
        }
      }
    }
    return instance;
  }

  /**
   * Get a specific meter and mark it
   * @param name name of meter/counter
   */
  public void markMeterAndCounter(String name) {
    MeterAndCounter meter = getOrCreateMeterAndCounter(name);
    meter.mark();
  }

}
