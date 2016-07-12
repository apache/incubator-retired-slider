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

package org.apache.slider.server.appmaster.model.appstate

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.hadoop.yarn.api.records.Priority
import org.apache.hadoop.yarn.api.records.Resource
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.InvalidContainerRequestException
import org.apache.slider.server.appmaster.state.ContainerPriority
import org.apache.slider.server.appmaster.state.OutstandingRequest
import org.apache.slider.test.SliderTestBase
import org.junit.Test

@CompileStatic
@Slf4j
class TestOutstandingRequestValidation extends SliderTestBase {

  final String[] H1 = hosts("one")

  @Test
  public void testRelaxedNohostsOrLabels() throws Throwable {
    createAndValidate(null, null, true)
  }

  @Test
  public void testRelaxedLabels() throws Throwable {
    createAndValidate(null, "gpu", true)
  }

  @Test
  public void testNonRelaxedLabels() throws Throwable {
    expectCreationFailure(null, "gpu", false)
  }

  @Test
  public void testRelaxedHostNoLabel() throws Throwable {
    createAndValidate(H1, "", true)
  }

  /**
   * Use varargs for simple list to array conversion
   * @param hostnames host names
   * @return
   */
  public static String[] hosts(String...hostnames) {
    hostnames
  }

  void expectCreationFailure(
    String[] hosts,
    String labels,
    boolean relaxLocality) {
    try {
      def result = createAndValidate(hosts, labels, relaxLocality)
      fail("Expected an exception, got $result")
    } catch (IllegalArgumentException expected) {
      assert expected.toString()
        .contains("Can't turn off locality relaxation on a request with no location constraints")
    }
  }


  AMRMClient.ContainerRequest createAndValidate(
    String[] hosts,
    String labels,
    boolean relaxLocality,
    int cores = 1,
    int memory = 64) {
    def p = 1
    Priority pri = ContainerPriority.createPriority(p, !relaxLocality);
    def issuedRequest = newRequest(pri, hosts, labels, relaxLocality)
    OutstandingRequest.validateContainerRequest(issuedRequest, p, "")
    issuedRequest
  }

  AMRMClient.ContainerRequest newRequest(
    Priority pri,
    String[] hosts,
    String labels,
    boolean relaxLocality,
    int cores = 1,
    int memory = 64) {
    Resource resource = Resource.newInstance(memory, cores)
    new AMRMClient.ContainerRequest(resource,
      hosts,
      null,
      pri,
      relaxLocality,
      labels);
  }

}
