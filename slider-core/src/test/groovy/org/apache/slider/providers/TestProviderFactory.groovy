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

package org.apache.slider.providers

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.slider.providers.agent.AgentKeys
import org.apache.slider.providers.agent.AgentProviderFactory
import org.junit.Test

@CompileStatic
@Slf4j
class TestProviderFactory {


  @Test
  public void testLoadAgentProvider() throws Throwable {
    SliderProviderFactory factory = SliderProviderFactory.createSliderProviderFactory(AgentKeys.PROVIDER_AGENT);
    assert factory instanceof AgentProviderFactory
  }

  @Test
  public void testCreateClientProvider() throws Throwable {
    SliderProviderFactory factory = SliderProviderFactory.createSliderProviderFactory(
        AgentKeys.PROVIDER_AGENT);
    assert null != factory.createClientProvider();
  }

  @Test
  public void testCreateHBaseProvider() throws Throwable {
    SliderProviderFactory factory = SliderProviderFactory.createSliderProviderFactory(
        AgentKeys.PROVIDER_AGENT);
    assert null != factory.createServerProvider();
  }
  
  @Test
  public void testCreateProviderByClassname() throws Throwable {
    SliderProviderFactory factory = SliderProviderFactory.createSliderProviderFactory(
        AgentProviderFactory.CLASSNAME);
    assert null != factory.createServerProvider();
    assert factory instanceof AgentProviderFactory
  }
  
  
}
