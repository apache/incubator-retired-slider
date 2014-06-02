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
import org.apache.hadoop.service.AbstractService;

import java.io.Closeable;
import java.io.IOException;

/**
 * Service that closes the closeable supplied during shutdown, if not null.
 */
public class ClosingService<C extends Closeable> extends AbstractService {

  private volatile C closeable;


  public ClosingService(String name,
      C closeable) {
    super(name);
    this.closeable = closeable;
  }

  public ClosingService(C closeable) {
    this("ClosingService", closeable);
  }


  public C getCloseable() {
    return closeable;
  }

  public void setCloseable(C closeable) {
    this.closeable = closeable;
  }

  /**
   * Stop routine will close the closeable -if not null - and set the
   * reference to null afterwards
   * This operation does raise any exception on the close, though it does
   * record it
   */
  @Override
  protected void serviceStop() {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException ioe) {
        noteFailure(ioe);
      }
      closeable = null;
    }
  }
}
