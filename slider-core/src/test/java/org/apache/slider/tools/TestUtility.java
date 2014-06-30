/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.slider.tools;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/** Various utility methods */
public class TestUtility {
  protected static final Logger log =
      LoggerFactory.getLogger(TestUtility.class);

  public static void addDir(File dirObj, ZipArchiveOutputStream zipFile, String prefix) throws IOException {
    for (File file : dirObj.listFiles()) {
      if (file.isDirectory()) {
        addDir(file, zipFile, prefix + file.getName() + File.separator);
      } else {
        log.info("Adding to zip - " + prefix + file.getName());
        zipFile.putArchiveEntry(new ZipArchiveEntry(prefix + file.getName()));
        IOUtils.copy(new FileInputStream(file), zipFile);
        zipFile.closeArchiveEntry();
      }
    }
  }

  public static void zipDir(String zipFile, String dir) throws IOException {
    File dirObj = new File(dir);
    ZipArchiveOutputStream out = new ZipArchiveOutputStream(new FileOutputStream(zipFile));
    log.info("Creating : " + zipFile);
    try {
      addDir(dirObj, out, "");
    } finally {
      out.close();
    }
  }

  public static String createAppPackage(
      TemporaryFolder folder, String subDir, String pkgName, String srcPath) throws IOException {
    String zipFileName;
    File pkgPath = folder.newFolder(subDir);
    File zipFile = new File(pkgPath, pkgName).getAbsoluteFile();
    zipFileName = zipFile.getAbsolutePath();
    TestUtility.zipDir(zipFileName, srcPath);
    log.info("Created temporary zip file at {}", zipFileName);
    return zipFileName;
  }

}
