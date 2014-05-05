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

package org.apache.slider.core.exceptions;

import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.UniformInterfaceException;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * static methods to convert exceptions into different types, including
 * extraction of details and finer-grained conversions.
 */
public class ExceptionConverter {

  /**
   * Convert a Jersey Exception into an IOE or subclass
   * @param targetURL URL being targeted 
   * @param exception original exception
   * @return a new exception, the original one nested as a cause
   */
  public static IOException convertJerseyException(String targetURL,
      UniformInterfaceException exception) {

    ClientResponse response = exception.getResponse();
    if (response != null) {
      int status = response.getStatus();
      if (status >= 400 && status < 500) {
        FileNotFoundException fnfe =
            new FileNotFoundException(targetURL);
        fnfe.initCause(exception);
        return fnfe;
      }
    }

    return new IOException("Failed to GET " + targetURL + ": " + exception, exception);
  }
}
