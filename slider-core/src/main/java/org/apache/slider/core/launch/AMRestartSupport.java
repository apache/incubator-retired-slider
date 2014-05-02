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

package org.apache.slider.core.launch;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.protocolrecords.impl.pb.RegisterApplicationMasterResponsePBImpl;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.slider.server.services.docstore.utility.SliderServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Logic needed to enable AM restart support in both submission and
 * operation
 */
public class AMRestartSupport {
  public static final String REGISTER_AM_RESPONSE =
    "RegisterApplicationMasterResponse.getContainersFromPreviousAttempts()";
  private static final Logger
    log = LoggerFactory.getLogger(SliderServiceUtils.class);

  /**
   * Request that containers are kept across submissions.
   * @param submissionContext context to query
   * @return true if the method was applied.
   */

  public static boolean keepContainersAcrossSubmissions(
    ApplicationSubmissionContext submissionContext) {
    Method m = null;
    String methName =
      "ApplicationSubmissionContext.setKeepContainersAcrossApplicationAttempts()";
    Class<? extends ApplicationSubmissionContext> cls =
      submissionContext.getClass();
    try {
      m = cls.getDeclaredMethod("setKeepContainersAcrossApplicationAttempts",
                                boolean.class);
      m.setAccessible(true);
    } catch (NoSuchMethodException e) {
      log.debug(methName + " not found");
    } catch (SecurityException e) {
      log.debug("No access to " + methName);
    }
    // AM-RESTART-SUPPORT: AM wants its old containers back on a restart
    if (m != null) {
      try {
        m.invoke(submissionContext, true);
        return true;
      } catch (InvocationTargetException ite) {
        log.error(methName + " got", ite);
      } catch (IllegalAccessException iae) {
        log.error(methName + " got", iae);
      }
    }
    return false;
  }

  /**
   * Get the containers from a previous attempt
   * @param response AM registration response
   * @return a list of containers (possibly empty) if the AM provided
   * that field in its registration, and the hadoop JAR has the relevant
   * method to access it.
   */
  public static List<Container> retrieveContainersFromPreviousAttempt(
    RegisterApplicationMasterResponse response) {
    List<Container> liveContainers = null;
    Method m = extractRetrieveContainersMethod(response);
    if (m != null) {
      try {
        Object obj = m.invoke(response);
        if (obj instanceof List) {
          liveContainers = (List<Container>) obj;

        }
      } catch (InvocationTargetException ite) {
        log.error(REGISTER_AM_RESPONSE + " got", ite);
      } catch (IllegalAccessException iae) {
        log.error(REGISTER_AM_RESPONSE + " got", iae);
      }
    }
    return liveContainers;
  }

  /**
   * Get the method to retrieve containers. The presence of this
   * method indicates the Hadoop libraries are compiled with the
   * extra fields, and that, if the client requested it, the AM
   * will be given a list of existing containers on a restart
   * @param response registration response
   * @return a method or null if it is not present.
   */
  public static Method extractRetrieveContainersMethod(
    RegisterApplicationMasterResponse response) {
    Method m = null;
    Class<? extends RegisterApplicationMasterResponse> cls =
      response.getClass();
    try {
      m = cls.getDeclaredMethod("getContainersFromPreviousAttempts");
    } catch (NoSuchMethodException e) {
      log.debug(REGISTER_AM_RESPONSE + " not found");
    } catch (SecurityException e) {
      log.debug("No access to " + REGISTER_AM_RESPONSE);
    }
    return m;
  }

  public static boolean isAMRestartInHadoopLibrary() {
    RegisterApplicationMasterResponse response =
      new RegisterApplicationMasterResponsePBImpl();
    return null != extractRetrieveContainersMethod(response);
  }
}
