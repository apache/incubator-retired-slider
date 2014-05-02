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

package org.apache.slider.registry.curator

import groovy.util.logging.Slf4j
import org.apache.curator.x.discovery.ServiceInstance
import org.apache.curator.x.discovery.ServiceInstanceBuilder
import org.apache.curator.x.discovery.UriSpec
import org.apache.slider.core.persist.JsonSerDeser
import org.apache.slider.core.registry.info.ServiceInstanceData
import org.apache.slider.server.services.curator.CuratorServiceInstance
import org.junit.Test

/**
 * Here to identify why curator was complaining about deserializing
 * its own published artifacts.
 *
 * ... its a Jackson version thing
 */
@Slf4j
class TestServiceInstanceSerDeser {

  JsonSerDeser<ServiceInstance> serializer = new JsonSerDeser<>(
      ServiceInstance.class)
  JsonSerDeser<CuratorServiceInstance> deser = new JsonSerDeser<>(
      CuratorServiceInstance.class)

  @Test
  public void testDefault() throws Throwable {
    def builder = ServiceInstance.builder()
    builder.name("defined")
    buildAndRoundTrip("testDefault", builder)
  }

  @Test
  public void testEmpty() throws Throwable {
    def builder = ServiceInstance.builder()
    builder.address(null).id("").name("").port(0).uriSpec(null)

    buildAndRoundTrip("testEmpty", builder)
  }

  @Test
  public void testFilled() throws Throwable {
    def builder = ServiceInstance.builder()

    builder.id("service").name("name").port(0)
           .uriSpec(new UriSpec("http:{}:{}"))


    buildAndRoundTrip("testFilled", builder)
  }

  @Test
  public void testPayload() throws Throwable {
    def builder = ServiceInstance.builder()
    builder.address(null).id("testPayload").name("").port(0).uriSpec(null)

    ServiceInstanceData data = new ServiceInstanceData()
    data.name = "testPayload"
    data.externalView.documentsURL = "http://documents"
    builder.payload(data)

    def instance = buildAndRoundTrip("", builder)

    def payload = instance.payload as ServiceInstanceData
    log.info("payload = $payload")
    assert payload.externalView.documentsURL == "http://documents"
  }

  @Test
  public void testHackedDeser() throws Throwable {
    def builder = ServiceInstance.builder()
    builder.address("localhost")
    builder.id("service").name("name").port(8080)
           .uriSpec(new UriSpec("http:{}:{}"))
    .sslPort(443)
    ServiceInstanceData data = new ServiceInstanceData()
    data.externalView.documentsURL = "http://documents"

    builder.payload(data)
    log.info("Test: testHackedDeser")
    String json = serialize(builder)
    CuratorServiceInstance<ServiceInstanceData> curatorInstance = deser.fromJson(json)
    log.info("resolved =$curatorInstance")
    def payload = curatorInstance.payload
    log.info("payload = $payload")
    assert payload.externalView.documentsURL == "http://documents"

  }

  def buildAndRoundTrip(String testname, ServiceInstanceBuilder builder) {
    log.info("Test: $testname")
    String json = serialize(builder)

    return serializer.fromJson(json)
  }

  def String serialize(ServiceInstanceBuilder builder) {
    ServiceInstance<ServiceInstanceData> instance = builder.build()
    def json = serializer.toJson(instance)
    log.info(json)
    return json
  }

}
