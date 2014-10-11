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

package org.apache.slider.server.appmaster.model.mock

import org.apache.hadoop.fs.FileAlreadyExistsException
import org.apache.hadoop.fs.PathIsNotEmptyDirectoryException
import org.apache.hadoop.fs.PathNotFoundException
import org.apache.hadoop.registry.client.api.RegistryOperations
import org.apache.hadoop.registry.client.exceptions.InvalidPathnameException
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException
import org.apache.hadoop.registry.client.exceptions.NoRecordException
import org.apache.hadoop.registry.client.types.RegistryPathStatus
import org.apache.hadoop.registry.client.types.ServiceRecord
import org.apache.hadoop.service.AbstractService

/**
 * Simple stub registry for when one is needed for its API, but the operations
 * are not actually required
 */
class MockRegistryOperations extends AbstractService implements RegistryOperations{

  MockRegistryOperations() {
    super("mock")
  }

  @Override
  boolean mknode(String path, boolean createParents)
  throws PathNotFoundException, InvalidPathnameException, IOException {
    return true
  }

  @Override
  void bind(String path, ServiceRecord record, int flags) throws
      PathNotFoundException,
      FileAlreadyExistsException,
      InvalidPathnameException,
      IOException {

  }

  @Override
  ServiceRecord resolve(String path) throws
      PathNotFoundException,
      NoRecordException,
      InvalidRecordException,
      IOException {
    throw new PathNotFoundException(path);
  }

  @Override
  RegistryPathStatus stat(String path)
  throws PathNotFoundException, InvalidPathnameException, IOException {
    throw new PathNotFoundException(path);
  }

  @Override
  boolean exists(String path) throws IOException {
    return false
  }

  @Override
  List<String> list(String path)
  throws PathNotFoundException, InvalidPathnameException, IOException {
    throw new PathNotFoundException(path);
  }

  @Override
  void delete(String path, boolean recursive) throws
      PathNotFoundException,
      PathIsNotEmptyDirectoryException,
      InvalidPathnameException,
      IOException {

  }

  @Override
  boolean addWriteAccessor(String id, String pass) throws IOException {
    return true
  }

  @Override
  void clearWriteAccessors() {

  }
}
