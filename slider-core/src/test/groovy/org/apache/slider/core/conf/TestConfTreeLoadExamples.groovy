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

package org.apache.slider.core.conf

import org.apache.slider.core.exceptions.BadConfigException
import org.apache.slider.core.persist.JsonSerDeser
import org.junit.Assert
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized


/**
 * Test 
 */
@RunWith(value = Parameterized.class)
class TestConfTreeLoadExamples extends Assert {

  String resource;

  static final JsonSerDeser<ConfTree> confTreeJsonSerDeser =
      new JsonSerDeser<ConfTree>(ConfTree)

  TestConfTreeLoadExamples(String resource) {
    this.resource = resource
  }

  @Parameterized.Parameters
  public static filenames() {
    return ExampleConfResources.all_example_resources.collect { [it] as String[] }
  }

  @Test
  public void testLoadResource() throws Throwable {
    def confTree = confTreeJsonSerDeser.fromResource(resource)
    ConfTreeOperations ops = new ConfTreeOperations(confTree)
    ops.resolve()
    ops.validate()

  }

  @Test
  public void testLoadResourceWithValidator() throws Throwable {
    def confTree = confTreeJsonSerDeser.fromResource(resource)
    ConfTreeOperations ops = new ConfTreeOperations(confTree)
    ops.resolve()
    if (resource.endsWith("resources.json")) {
      // these should pass since they are configured conrrectly with "yarn."
      // properties
      ops.validate(new ResourcesInputPropertiesValidator())
    } else if (resource.startsWith("app_configuration")) {
      ops.validate(new TemplateInputPropertiesValidator())
    }
    else {
      // these have properties with other prefixes so they should generate
      // BadConfigExceptions
      try {
        ops.validate(new ResourcesInputPropertiesValidator())
        if ( !resource.endsWith(ExampleConfResources.empty)) {
          fail (resource + " should have generated validation exception")
        }
      } catch (BadConfigException e) {
         // ignore
      }

    }
  }
}
