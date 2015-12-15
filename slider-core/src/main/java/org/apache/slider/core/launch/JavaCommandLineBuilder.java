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


import com.google.common.base.Preconditions;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.slider.common.tools.SliderUtils;

/**
 * Command line builder purely for the Java CLI
 */
public class JavaCommandLineBuilder extends CommandLineBuilder {

  public JavaCommandLineBuilder() {
    add(getJavaBinary());
  }

  /**
   * Get the java binary. This is called in the constructor so don't try and
   * do anything other than return a constant.
   * @return the path to the Java binary
   */
  protected String getJavaBinary() {
    return ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java";
  }

  /**
   * Set the size of the heap if a non-empty heap is passed in. 
   * @param heap empty string or something like "128M" ,"1G" etc. The value is
   * trimmed.
   */
  public void setJVMHeap(String heap) {
    if (SliderUtils.isSet(heap)) {
      add("-Xmx" + heap.trim());
    }
  }

  /**
   * Turn Java assertions on
   */
  public void enableJavaAssertions() {
    add("-ea");
    add("-esa");
  }

  /**
   * Add a system property definition -must be used before setting the main entry point
   * @param property
   * @param value
   */
  public void sysprop(String property, String value) {
    Preconditions.checkArgument(property != null, "null property name");
    Preconditions.checkArgument(value != null, "null value");
    add("-D" + property + "=" + value);
  }
  
  public JavaCommandLineBuilder forceIPv4() {
    sysprop("java.net.preferIPv4Stack", "true");
    return this;
  }
  
  public JavaCommandLineBuilder headless() {
    sysprop("java.awt.headless", "true");
    return this;
  }
}
