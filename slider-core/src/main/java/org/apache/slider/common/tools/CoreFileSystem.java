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

package org.apache.slider.common.tools;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.slider.common.SliderExitCodes;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException;
import org.apache.slider.core.persist.Filenames;
import org.apache.slider.core.persist.InstancePaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.apache.slider.common.SliderXmlConfKeys.CLUSTER_DIRECTORY_PERMISSIONS;
import static org.apache.slider.common.SliderXmlConfKeys.DEFAULT_CLUSTER_DIRECTORY_PERMISSIONS;

public class CoreFileSystem {
  private static final Logger
    log = LoggerFactory.getLogger(CoreFileSystem.class);

  protected final FileSystem fileSystem;
  protected final Configuration configuration;

  public CoreFileSystem(FileSystem fileSystem, Configuration configuration) {
    Preconditions.checkNotNull(fileSystem,
                               "Cannot create a CoreFileSystem with a null FileSystem");
    Preconditions.checkNotNull(configuration,
                               "Cannot create a CoreFileSystem with a null Configuration");
    this.fileSystem = fileSystem;
    this.configuration = configuration;
  }

  public CoreFileSystem(Configuration configuration) throws IOException {
    Preconditions.checkNotNull(configuration,
                               "Cannot create a CoreFileSystem with a null Configuration");
    this.fileSystem = FileSystem.get(configuration);
    this.configuration = fileSystem.getConf();
  }
  
  /**
   * Get the temp path for this cluster
   * @param clustername name of the cluster
   * @return path for temp files (is not purged)
   */
  public Path getTempPathForCluster(String clustername) {
    Path clusterDir = buildClusterDirPath(clustername);
    return new Path(clusterDir, SliderKeys.TMP_DIR_PREFIX);
  }

  /**
   * Returns the underlying FileSystem for this object.
   *
   * @return filesystem
   */
  public FileSystem getFileSystem() {
    return fileSystem;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
      new StringBuilder("CoreFileSystem{");
    sb.append("fileSystem=").append(fileSystem.getUri());
    sb.append('}');
    return sb.toString();
  }

  /**
   * Build up the path string for a cluster instance -no attempt to
   * create the directory is made
   *
   * @param clustername name of the cluster
   * @return the path for persistent data
   */
  public Path buildClusterDirPath(String clustername) {
    if (clustername == null) {
      throw new NullPointerException();
    }
    Path path = getBaseApplicationPath();
    return new Path(path, SliderKeys.CLUSTER_DIRECTORY + "/" + clustername);
  }

  /**
   * Create the Slider cluster path for a named cluster and all its subdirs
   * This is a directory; a mkdirs() operation is executed
   * to ensure that it is there.
   *
   * @param clustername name of the cluster
   * @return the path to the cluster directory
   * @throws java.io.IOException                      trouble
   * @throws SliderException slider-specific exceptions
   */
  public Path createClusterDirectories(String clustername, Configuration conf) throws
                                                                               IOException,
      SliderException {
    
    
    Path clusterDirectory = buildClusterDirPath(clustername);
    InstancePaths instancePaths = new InstancePaths(clusterDirectory);
    createClusterDirectories(instancePaths);
    return clusterDirectory;
  }
  
  /**
   * Create the Slider cluster path for a named cluster and all its subdirs
   * This is a directory; a mkdirs() operation is executed
   * to ensure that it is there.
   *
   * @param instancePaths instance paths
   * @return the path to the cluster directory
   * @throws java.io.IOException                      trouble
   * @throws SliderException slider-specific exceptions
   */
  public void createClusterDirectories(InstancePaths instancePaths) throws
      IOException, SliderException {
    Path clusterDirectory = instancePaths.instanceDir;

    verifyDirectoryNonexistent(clusterDirectory);
    FsPermission clusterPerms = getInstanceDirectoryPermissions();
    createWithPermissions(clusterDirectory, clusterPerms);
    createWithPermissions(instancePaths.snapshotConfPath, clusterPerms);
    createWithPermissions(instancePaths.generatedConfPath, clusterPerms);
    createWithPermissions(instancePaths.historyPath, clusterPerms);
    createWithPermissions(instancePaths.tmpPathAM, clusterPerms);

    // Data Directory
    String dataOpts =
      configuration.get(SliderXmlConfKeys.DATA_DIRECTORY_PERMISSIONS,
               SliderXmlConfKeys.DEFAULT_DATA_DIRECTORY_PERMISSIONS);
    log.debug("Setting data directory permissions to {}", dataOpts);
    createWithPermissions(instancePaths.dataPath, new FsPermission(dataOpts));

  }

  /**
   * Create a directory with the given permissions.
   *
   * @param dir          directory
   * @param clusterPerms cluster permissions
   * @throws IOException                                 IO problem
   * @throws org.apache.slider.core.exceptions.BadClusterStateException any cluster state problem
   */
  public void createWithPermissions(Path dir, FsPermission clusterPerms) throws
          IOException,
          BadClusterStateException {
    if (fileSystem.isFile(dir)) {
      // HADOOP-9361 shows some filesystems don't correctly fail here
      throw new BadClusterStateException(
              "Cannot create a directory over a file %s", dir);
    }
    log.debug("mkdir {} with perms {}", dir, clusterPerms);
    //no mask whatoever
    fileSystem.getConf().set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "000");
    fileSystem.mkdirs(dir, clusterPerms);
    //and force set it anyway just to make sure
    fileSystem.setPermission(dir, clusterPerms);
  }

  /**
   * Get the permissions of a path
   *
   * @param path path to check
   * @return the permissions
   * @throws IOException any IO problem (including file not found)
   */
  public FsPermission getPathPermissions(Path path) throws IOException {
    FileStatus status = fileSystem.getFileStatus(path);
    return status.getPermission();
  }

  public FsPermission getInstanceDirectoryPermissions() {
    String clusterDirPermsOct =
      configuration.get(CLUSTER_DIRECTORY_PERMISSIONS,
                        DEFAULT_CLUSTER_DIRECTORY_PERMISSIONS);
    return new FsPermission(clusterDirPermsOct);
  }

  /**
   * Verify that the cluster directory is not present
   *
   * @param clustername      name of the cluster
   * @param clusterDirectory actual directory to look for
   * @return the path to the cluster directory
   * @throws IOException                      trouble with FS
   * @throws SliderException If the directory exists
   */
  public void verifyClusterDirectoryNonexistent(String clustername,
                                                Path clusterDirectory) throws
          IOException,
      SliderException {
    if (fileSystem.exists(clusterDirectory)) {
      throw new SliderException(SliderExitCodes.EXIT_INSTANCE_EXISTS,
              ErrorStrings.PRINTF_E_INSTANCE_ALREADY_EXISTS, clustername,
              clusterDirectory);
    }
  }
  /**
   * Verify that the given directory is not present
   *
   * @param clusterDirectory actual directory to look for
   * @return the path to the cluster directory
   * @throws IOException    trouble with FS
   * @throws SliderException If the directory exists
   */
  public void verifyDirectoryNonexistent(Path clusterDirectory) throws
          IOException,
      SliderException {
    if (fileSystem.exists(clusterDirectory)) {
      log.error("Dir {} exists: {}",
                clusterDirectory,
                listFSDir(clusterDirectory));
      throw new SliderException(SliderExitCodes.EXIT_INSTANCE_EXISTS,
              ErrorStrings.PRINTF_E_INSTANCE_DIR_ALREADY_EXISTS,
              clusterDirectory);
    }
  }

  /**
   * Verify that a user has write access to a directory.
   * It does this by creating then deleting a temp file
   *
   * @param dirPath actual directory to look for
   * @throws FileNotFoundException file not found
   * @throws IOException  trouble with FS
   * @throws BadClusterStateException if the directory is not writeable
   */
  public void verifyDirectoryWriteAccess(Path dirPath) throws IOException,
      SliderException {
    verifyPathExists(dirPath);
    Path tempFile = new Path(dirPath, "tmp-file-for-checks");
    try {
      FSDataOutputStream out = null;
      out = fileSystem.create(tempFile, true);
      IOUtils.closeStream(out);
      fileSystem.delete(tempFile, false);
    } catch (IOException e) {
      log.warn("Failed to create file {}: {}", tempFile, e);
      throw new BadClusterStateException(e,
              "Unable to write to directory %s : %s", dirPath, e.toString());
    }
  }

  /**
   * Verify that a path exists
   * @param path path to check
   * @throws FileNotFoundException file not found
   * @throws IOException  trouble with FS
   */
  public void verifyPathExists(Path path) throws IOException {
    if (!fileSystem.exists(path)) {
      throw new FileNotFoundException(path.toString());
    }
  }

  /**
   * Verify that a path exists
   * @param path path to check
   * @throws FileNotFoundException file not found or is not a file
   * @throws IOException  trouble with FS
   */
  public void verifyFileExists(Path path) throws IOException {
    FileStatus status = fileSystem.getFileStatus(path);

    if (!status.isFile()) {
      throw new FileNotFoundException("Not a file: " + path.toString());
    }
  }

  /**
   * Create the application-instance specific temporary directory
   * in the DFS
   *
   * @param clustername name of the cluster
   * @param subdir       application ID
   * @return the path; this directory will already have been created
   */
  public Path createAppInstanceTempPath(String clustername, String subdir)
      throws IOException {
    Path tmp = getTempPathForCluster(clustername);
    Path instancePath = new Path(tmp, subdir);
    fileSystem.mkdirs(instancePath);
    return instancePath;
  }

  /**
   * Create the application-instance specific temporary directory
   * in the DFS
   *
   * @param clustername name of the cluster
   * @return the path; this directory will already have been deleted
   */
  public Path purgeAppInstanceTempFiles(String clustername) throws
          IOException {
    Path tmp = getTempPathForCluster(clustername);
    fileSystem.delete(tmp, true);
    return tmp;
  }

  /**
   * Get the base path
   *
   * @return the base path optionally configured by {@value SliderXmlConfKeys#KEY_SLIDER_BASE_PATH}
   */
  public Path getBaseApplicationPath() {
    String configuredBasePath = configuration.get(SliderXmlConfKeys.KEY_SLIDER_BASE_PATH);
    return configuredBasePath != null ? new Path(configuredBasePath) :
           new Path(getHomeDirectory(), SliderKeys.SLIDER_BASE_DIRECTORY);
  }

  public Path getHomeDirectory() {
    return fileSystem.getHomeDirectory();
  }

  public boolean maybeAddImagePath(Map<String, LocalResource> localResources,
                                   Path imagePath) throws IOException {
    if (imagePath != null) {
      LocalResource resource = createAmResource(imagePath,
          LocalResourceType.ARCHIVE);
      localResources.put(SliderKeys.LOCAL_TARBALL_INSTALL_SUBDIR, resource);
      return true;
    } else {
      return false;
    }
  }

  public boolean maybeAddImagePath(Map<String, LocalResource> localResources,
                                   String imagePath) throws IOException {
    
    return imagePath != null &&
           maybeAddImagePath(localResources, new Path(imagePath));
  }
  
  
  

  /**
   * Create an AM resource from the
   *
   * @param destPath     dest path in filesystem
   * @param resourceType resource type
   * @return the resource set up wih application-level visibility and the
   * timestamp & size set from the file stats.
   */
  public LocalResource createAmResource(Path destPath, LocalResourceType resourceType) throws IOException {
    FileStatus destStatus = fileSystem.getFileStatus(destPath);
    LocalResource amResource = Records.newRecord(LocalResource.class);
    amResource.setType(resourceType);
    // Set visibility of the resource
    // Setting to most private option
    amResource.setVisibility(LocalResourceVisibility.APPLICATION);
    // Set the resource to be copied over
    amResource.setResource(ConverterUtils.getYarnUrlFromPath(destPath));
    // Set timestamp and length of file so that the framework
    // can do basic sanity checks for the local resource
    // after it has been copied over to ensure it is the same
    // resource the client intended to use with the application
    amResource.setTimestamp(destStatus.getModificationTime());
    amResource.setSize(destStatus.getLen());
    return amResource;
  }

  /**
   * Register all files under a fs path as a directory to push out
   *
   * @param srcDir          src dir
   * @param destRelativeDir dest dir (no trailing /)
   * @return the map of entries
   */
  public Map<String, LocalResource> submitDirectory(Path srcDir, String destRelativeDir) throws IOException {
    //now register each of the files in the directory to be
    //copied to the destination
    FileStatus[] fileset = fileSystem.listStatus(srcDir);
    Map<String, LocalResource> localResources =
            new HashMap<String, LocalResource>(fileset.length);
    for (FileStatus entry : fileset) {

      LocalResource resource = createAmResource(entry.getPath(),
              LocalResourceType.FILE);
      String relativePath = destRelativeDir + "/" + entry.getPath().getName();
      localResources.put(relativePath, resource);
    }
    return localResources;
  }

  /**
   * Submit a JAR containing a specific class, returning
   * the resource to be mapped in
   *
   * @param clazz   class to look for
   * @param subdir  subdirectory (expected to end in a "/")
   * @param jarName <i>At the destination</i>
   * @return the local resource ref
   * @throws IOException trouble copying to HDFS
   */
  public LocalResource submitJarWithClass(Class clazz, Path tempPath, String subdir, String jarName)
          throws IOException, SliderException {
    File localFile = SliderUtils.findContainingJarOrFail(clazz);
    LocalResource resource = submitFile(localFile, tempPath, subdir, jarName);
    return resource;
  }

  /**
   * Submit a local file to the filesystem references by the instance's cluster
   * filesystem
   *
   * @param localFile    filename
   * @param subdir       subdirectory (expected to end in a "/")
   * @param destFileName destination filename
   * @return the local resource ref
   * @throws IOException trouble copying to HDFS
   */
  public LocalResource submitFile(File localFile, Path tempPath, String subdir, String destFileName) throws IOException {
    Path src = new Path(localFile.toString());
    Path subdirPath = new Path(tempPath, subdir);
    fileSystem.mkdirs(subdirPath);
    Path destPath = new Path(subdirPath, destFileName);

    fileSystem.copyFromLocalFile(false, true, src, destPath);

    // Set the type of resource - file or archive
    // archives are untarred at destination
    // we don't need the jar file to be untarred for now
    return createAmResource(destPath, LocalResourceType.FILE);
  }

  /**
   * list entries in a filesystem directory
   *
   * @param path directory
   * @return a listing, one to a line
   * @throws IOException
   */
  public String listFSDir(Path path) throws IOException {
    FileStatus[] stats = fileSystem.listStatus(path);
    StringBuilder builder = new StringBuilder();
    for (FileStatus stat : stats) {
      builder.append(stat.getPath().toString())
              .append("\t")
              .append(stat.getLen())
              .append("\n");
    }
    return builder.toString();
  }

  public void touch(Path path, boolean overwrite) throws IOException {
    FSDataOutputStream out = fileSystem.create(path, overwrite);
    out.close();
  }

  public void cat(Path path, boolean overwrite, String data) throws IOException {
    FSDataOutputStream out = fileSystem.create(path, overwrite);
    byte[] bytes = data.getBytes(Charset.forName("UTF-8"));
    out.write(bytes);
    out.close();
  }

  /**
   * Create a path that must exist in the cluster fs
   * @param uri uri to create
   * @return the path
   * @throws SliderException if the path does not exist
   */
  public Path createPathThatMustExist(String uri) throws
      SliderException,
                                                  IOException {
    Path path = new Path(uri);
    verifyPathExists(path);
    return path;
  }

  /**
   * Locate an application conf json in the FS. This includes a check to verify
   * that the file is there.
   *
   * @param clustername name of the cluster
   * @return the path to the spec.
   * @throws IOException                      IO problems
   * @throws SliderException if the path isn't there
   */
  public Path locateInstanceDefinition(String clustername) throws IOException,
      SliderException {
    Path clusterDirectory = buildClusterDirPath(clustername);
    Path appConfPath =
            new Path(clusterDirectory, Filenames.APPCONF);
    verifyClusterSpecExists(clustername, appConfPath);
    return appConfPath;
  }

  /**
   * Verify that a cluster specification exists
   * @param clustername name of the cluster (For errors only)
   * @param clusterSpecPath cluster specification path
   * @throws IOException IO problems
   * @throws SliderException if the cluster specification is not present
   */
  public void verifyClusterSpecExists(String clustername,
                                             Path clusterSpecPath) throws
                                                                   IOException,
      SliderException {
    if (!fileSystem.isFile(clusterSpecPath)) {
      log.debug("Missing specification file {}", clusterSpecPath);
      throw UnknownApplicationInstanceException.unknownInstance(clustername
                                                                +
                                                                "\n (definition not found at "
                                                                +
                                                                clusterSpecPath);
    }
  }
}
