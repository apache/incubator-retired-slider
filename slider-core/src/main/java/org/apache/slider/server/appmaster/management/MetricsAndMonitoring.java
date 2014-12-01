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
}
