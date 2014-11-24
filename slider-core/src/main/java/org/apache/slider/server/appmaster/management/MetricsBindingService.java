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
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.ganglia.GangliaReporter;
import info.ganglia.gmetric4j.gmetric.GMetric;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.service.AbstractService;

import java.util.concurrent.TimeUnit;

/**
 * YARN service which hooks up Codahale metrics to 
 * Ganglia (if enabled)
 */
public class MetricsBindingService extends AbstractService {

  /**
   * {@value}
   */
  public static final String METRICS_GANGLIA_ENABLED =
      "metrics.ganglia.enabled";
  /**
   * {@value}
   */
  public static final String METRICS_GANGLIA_HOST = "metrics.ganglia.host";

  /**
   * {@value}
   */
  public static final String METRICS_GANGLIA_PORT = "metrics.ganglia.port";

  /**
   * {@value}
   */
  public static final String METRICS_GANGLIA_VERSION_31 = "metrics.ganglia.version.31";

  /**
   * {@value}
   */
  public static final String METRICS_GANGLIA_REPORT_INTERVAL = "metrics.ganglia.report.interval";

  /**
   * {@value}
   */
  public static final int DEFAULT_GANGLIA_PORT = 8649;

  private final MetricRegistry metrics;
  private ScheduledReporter reporter;

  public MetricsBindingService(String name,
      MetricRegistry metrics) {
    super(name);
    this.metrics = metrics;
  }

  /**
   * Instantiate...create a metric registry in the process
   * @param name service name
   */
  public MetricsBindingService(String name) {
    this(name, new MetricRegistry());
  }

  public MetricRegistry getMetrics() {
    return metrics;
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    Configuration conf = getConfig();
    boolean enabled = conf.getBoolean(METRICS_GANGLIA_ENABLED, false);

    if (enabled) {
      String host = conf.getTrimmed(METRICS_GANGLIA_HOST, "");
      int port = conf.getInt(METRICS_GANGLIA_PORT, DEFAULT_GANGLIA_PORT);
      int interval = conf.getInt(METRICS_GANGLIA_REPORT_INTERVAL, 60);
      int ttl = 1;
      GMetric.UDPAddressingMode
          mcast = GMetric.UDPAddressingMode.getModeForAddress(host);
      boolean ganglia31 = conf.getBoolean(METRICS_GANGLIA_VERSION_31, true);

      final GMetric ganglia =
          new GMetric(
              host, 
              port,
              mcast,
              ttl,
              ganglia31);
      reporter = GangliaReporter.forRegistry(metrics)
                                .convertRatesTo(TimeUnit.SECONDS)
                                .convertDurationsTo(TimeUnit.MILLISECONDS)
                                .build(ganglia);
      reporter.start(interval, TimeUnit.SECONDS);
    }
  }

  @Override
  protected void serviceStop() throws Exception {
    IOUtils.closeStream(reporter);
    super.serviceStop();
  }
  
  public boolean isEnabled() {
    return reporter != null;
  }

}
