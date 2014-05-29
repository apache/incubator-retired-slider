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

package org.apache.slider.server.services.workflow;

import com.google.common.base.Preconditions;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread factory that creates threads (possibly daemon threads)
 * using the name and naming policy supplied.
 * The thread counter starts at 1, increments atomically, 
 * and is supplied as the second argument in the format string
 */
public class ServiceThreadFactory implements ThreadFactory {

  private static AtomicInteger counter = new AtomicInteger(1);
  public static final String DEFAULT_NAMING_FORMAT = "%s-%03d";
  private final String name;
  private final boolean daemons;
  private final String namingFormat;

  public ServiceThreadFactory(String name,
      boolean daemons,
      String namingFormat) {
    Preconditions.checkNotNull(name, "null name");
    Preconditions.checkNotNull(namingFormat, "null naming format");
    this.name = name;
    this.daemons = daemons;
    this.namingFormat = namingFormat;
  }

  public ServiceThreadFactory(String name,
      boolean daemons) {
    this(name, daemons, DEFAULT_NAMING_FORMAT);
  }

  @Override
  public Thread newThread(Runnable r) {
    Preconditions.checkNotNull(r, "null runnable");
    String threadName =
        String.format(namingFormat, name, counter.getAndIncrement());
    return new Thread(r, threadName);
  }

}
