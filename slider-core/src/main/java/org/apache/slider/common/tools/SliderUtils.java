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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.VersionInfo;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.slider.api.OptionKeys;
import org.apache.slider.api.RoleKeys;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.BadCommandArgumentsException;
import org.apache.slider.core.exceptions.BadConfigException;
import org.apache.slider.core.exceptions.ErrorStrings;
import org.apache.slider.core.exceptions.MissingArgException;
import org.apache.slider.core.exceptions.SliderException;
import org.apache.slider.core.launch.ClasspathConstructor;
import org.apache.zookeeper.server.util.KerberosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * These are slider-specific Util methods
 */
public final class SliderUtils {

  private static final Logger log = LoggerFactory.getLogger(SliderUtils.class);

  /**
   * Atomic bool to track whether or not process security has already been
   * turned on (prevents re-entrancy)
   */
  private static final AtomicBoolean processSecurityAlreadyInitialized =
    new AtomicBoolean(false);
  public static final String JAVA_SECURITY_KRB5_REALM =
      "java.security.krb5.realm";
  public static final String JAVA_SECURITY_KRB5_KDC = "java.security.krb5.kdc";

  private SliderUtils() {
  }

  /**
   * Implementation of set-ness, groovy definition of true/false for a string
   * @param s string
   * @return true iff the string is neither null nor empty
   */
  public static boolean isUnset(String s) {
    return s == null || s.isEmpty();
  }

  public static boolean isSet(String s) {
    return !isUnset(s);
  }

  /*
   * Validates whether num is an integer
   * @param num
   * @param msg the message to be shown in exception
   */
  private static void validateNumber(String num, String msg)  throws BadConfigException {
    try {
      Integer.parseInt(num);
    } catch (NumberFormatException nfe) {
      throw new BadConfigException(msg + num);
    }
  }

  /*
   * Translates the trailing JVM heapsize unit: g, G, m, M
   * This assumes designated unit of 'm'
   * @param heapsize
   * @return heapsize in MB
   */
  public static String translateTrailingHeapUnit(String heapsize) throws BadConfigException {
    String errMsg = "Bad heapsize: ";
    if (heapsize.endsWith("m") || heapsize.endsWith("M")) {
      String num = heapsize.substring(0, heapsize.length()-1);
      validateNumber(num, errMsg);
      return num;
    }
    if (heapsize.endsWith("g") || heapsize.endsWith("G")) {
      String num = heapsize.substring(0, heapsize.length()-1)+"000";
      validateNumber(num, errMsg);
      return num;
    }
    // check if specified heap size is a number
    validateNumber(heapsize, errMsg);
    return heapsize;
  }

  /**
   * recursive directory delete
   * @param dir dir to delete
   * @throws IOException on any problem
   */
  public static void deleteDirectoryTree(File dir) throws IOException {
    if (dir.exists()) {
      if (dir.isDirectory()) {
        log.info("Cleaning up {}", dir);
        //delete the children
        File[] files = dir.listFiles();
        if (files == null) {
          throw new IOException("listfiles() failed for " + dir);
        }
        for (File file : files) {
          log.info("deleting {}", file);
          if (!file.delete()) {
            log.warn("Unable to delete " + file);
          }
        }
        if (!dir.delete()) {
          log.warn("Unable to delete " + dir);
        }
      } else {
        throw new IOException("Not a directory " + dir);
      }
    } else {
      //not found, do nothing
      log.debug("No output dir yet");
    }
  }

  /**
   * Find a containing JAR
   * @param my_class class to find
   * @return the file
   * @throws IOException any IO problem, including the class not having a
   * classloader
   * @throws FileNotFoundException if the class did not resolve to a file
   */
  public static File findContainingJarOrFail(Class clazz) throws IOException {
    File localFile = SliderUtils.findContainingJar(clazz);
    if (null == localFile) {
      throw new FileNotFoundException("Could not find JAR containing " + clazz);
    }
    return localFile;
  }


  /**
   * Find a containing JAR
   * @param my_class class to find
   * @return the file or null if it is not found
   * @throws IOException any IO problem, including the class not having a
   * classloader
   */
  public static File findContainingJar(Class my_class) throws IOException {
    ClassLoader loader = my_class.getClassLoader();
    if (loader == null) {
      throw new IOException(
        "Class " + my_class + " does not have a classloader!");
    }
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    Enumeration<URL> urlEnumeration = loader.getResources(class_file);
    if (urlEnumeration == null) {
      throw new IOException("Unable to find resources for class " + my_class);
    }

    for (Enumeration itr = urlEnumeration; itr.hasMoreElements(); ) {
      URL url = (URL) itr.nextElement();
      if ("jar".equals(url.getProtocol())) {
        String toReturn = url.getPath();
        if (toReturn.startsWith("file:")) {
          toReturn = toReturn.substring("file:".length());
        }
        // URLDecoder is a misnamed class, since it actually decodes
        // x-www-form-urlencoded MIME type rather than actual
        // URL encoding (which the file path has). Therefore it would
        // decode +s to ' 's which is incorrect (spaces are actually
        // either unencoded or encoded as "%20"). Replace +s first, so
        // that they are kept sacred during the decoding process.
        toReturn = toReturn.replaceAll("\\+", "%2B");
        toReturn = URLDecoder.decode(toReturn, "UTF-8");
        String jarFilePath = toReturn.replaceAll("!.*$", "");
        return new File(jarFilePath);
      } else {
        log.info("could not locate JAR containing {} URL={}", my_class, url);
      }
    }
    return null;
  }

  public static void checkPort(String hostname, int port, int connectTimeout)
    throws IOException {
    InetSocketAddress addr = new InetSocketAddress(hostname, port);
    checkPort(hostname, addr, connectTimeout);
  }

  @SuppressWarnings("SocketOpenedButNotSafelyClosed")
  public static void checkPort(String name,
                               InetSocketAddress address,
                               int connectTimeout)
    throws IOException {
    Socket socket = null;
    try {
      socket = new Socket();
      socket.connect(address, connectTimeout);
    } catch (Exception e) {
      throw new IOException("Failed to connect to " + name
                            + " at " + address
                            + " after " + connectTimeout + "millisconds"
                            + ": " + e,
                            e);
    } finally {
      IOUtils.closeSocket(socket);
    }
  }

  public static void checkURL(String name, String url, int timeout) throws
                                                                    IOException {
    InetSocketAddress address = NetUtils.createSocketAddr(url);
    checkPort(name, address, timeout);
  }

  /**
   * A required file
   * @param role role of the file (for errors)
   * @param filename the filename
   * @throws ExitUtil.ExitException if the file is missing
   * @return the file
   */
  public static File requiredFile(String filename, String role) throws
                                                                IOException {
    if (filename.isEmpty()) {
      throw new ExitUtil.ExitException(-1, role + " file not defined");
    }
    File file = new File(filename);
    if (!file.exists()) {
      throw new ExitUtil.ExitException(-1,
                                       role + " file not found: " +
                                       file.getCanonicalPath());
    }
    return file;
  }

  /**
   * Normalize a cluster name then verify that it is valid
   * @param name proposed cluster name
   * @return true iff it is valid
   */
  public static boolean isClusternameValid(String name) {
    if (name == null || name.isEmpty()) {
      return false;
    }
    int first = name.charAt(0);
    if (0 == (Character.getType(first)  & Character.LOWERCASE_LETTER)) {
      return false;
    }

    for (int i = 0; i < name.length(); i++) {
      int elt = (int) name.charAt(i);
      int t = Character.getType(elt);
      if (0 == (t & Character.LOWERCASE_LETTER)
          && 0 == (t & Character.DECIMAL_DIGIT_NUMBER)
          && elt != '-'
          && elt != '_') {
        return false;
      }
      if (!Character.isLetterOrDigit(elt) && elt != '-' && elt != '_') {
        return false;
      }
    }
    return true;
  }

  /**
   * Copy a directory to a new FS -both paths must be qualified. If
   * a directory needs to be created, supplied permissions can override
   * the default values. Existing directories are not touched
   * @param conf conf file
   * @param srcDirPath src dir
   * @param destDirPath dest dir
   * @param permission permission for the dest directory; null means "default"
   * @return # of files copies
   */
  public static int copyDirectory(Configuration conf,
                                  Path srcDirPath,
                                  Path destDirPath,
                                  FsPermission permission) throws
                                                           IOException,
                                                           BadClusterStateException {
    FileSystem srcFS = FileSystem.get(srcDirPath.toUri(), conf);
    FileSystem destFS = FileSystem.get(destDirPath.toUri(), conf);
    //list all paths in the src.
    if (!srcFS.exists(srcDirPath)) {
      throw new FileNotFoundException("Source dir not found " + srcDirPath);
    }
    if (!srcFS.isDirectory(srcDirPath)) {
      throw new FileNotFoundException("Source dir not a directory " + srcDirPath);
    }
    FileStatus[] entries = srcFS.listStatus(srcDirPath);
    int srcFileCount = entries.length;
    if (srcFileCount == 0) {
      return 0;
    }
    if (permission == null) {
      permission = FsPermission.getDirDefault();
    }
    if (!destFS.exists(destDirPath)) {
      new SliderFileSystem(destFS, conf).createWithPermissions(destDirPath, permission);
    }
    Path[] sourcePaths = new Path[srcFileCount];
    for (int i = 0; i < srcFileCount; i++) {
      FileStatus e = entries[i];
      Path srcFile = e.getPath();
      if (srcFS.isDirectory(srcFile)) {
        throw new IOException("Configuration dir " + srcDirPath
                              + " contains a directory " + srcFile);
      }
      log.debug("copying src conf file {}", srcFile);
      sourcePaths[i] = srcFile;
    }
    log.debug("Copying {} files from {} to dest {}", srcFileCount,
              srcDirPath,
              destDirPath);
    FileUtil.copy(srcFS, sourcePaths, destFS, destDirPath, false, true, conf);
    return srcFileCount;
  }


  public static String stringify(Throwable t) {
    StringWriter sw = new StringWriter();
    sw.append(t.toString()).append('\n');
    t.printStackTrace(new PrintWriter(sw));
    return sw.toString();
  }

  /**
   * Create a configuration with Slider-specific tuning.
   * This is done rather than doing custom configs.
   * @return the config
   */
  public static YarnConfiguration createConfiguration() {
    YarnConfiguration conf = new YarnConfiguration();
    patchConfiguration(conf);
    return conf;
  }

  /**
   * Take an existing conf and patch it for Slider's needs. Useful
   * in Service.init & RunService methods where a shared config is being
   * passed in
   * @param conf configuration
   * @return the patched configuration
   */
  public static Configuration patchConfiguration(Configuration conf) {

    //if the fallback option is NOT set, enable it.
    //if it is explicitly set to anything -leave alone
    if (conf.get(SliderXmlConfKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH) == null) {
      conf.set(SliderXmlConfKeys.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH, "true");
    }
    return conf;
  }

  /**
   * Take a collection, return a list containing the string value of every
   * element in the collection.
   * @param c collection
   * @return a stringified list
   */
  public static List<String> collectionToStringList(Collection c) {
    List<String> l = new ArrayList<String>(c.size());
    for (Object o : c) {
      l.add(o.toString());
    }
    return l;
  }

  /**
   * Join an collection of objects with a separator that appears after every
   * instance in the list -including at the end
   * @param collection collection to call toString() on each element
   * @param separator separator string
   * @return the joined entries
   */
  public static String join(Collection collection, String separator) {
    return join(collection, separator, true);
  }

  /**
   * Join an collection of objects with a separator that appears after every
   * instance in the list -optionally at the end
   * @param collection collection to call toString() on each element
   * @param separator separator string
   * @param trailing add a trailing entry or not
   * @return the joined entries
   */
  public static String join(Collection collection, String separator, boolean trailing) {
    StringBuilder b = new StringBuilder();
    for (Object o : collection) {
      b.append(o);
      b.append(separator);
    }
    return trailing? 
           b.toString()
           : (b.substring(0, b.length() - 1));
  }

  /**
   * Join an array of strings with a separator that appears after every
   * instance in the list -including at the end
   * @param collection strings
   * @param separator separator string
   * @return the joined entries
   */
  public static String join(String[] collection, String separator) {
    return join(collection, separator, true);
    
    
  }
  /**
   * Join an array of strings with a separator that appears after every
   * instance in the list -optionally at the end
   * @param collection strings
   * @param separator separator string
   * @param trailing add a trailing entry or not
   * @return the joined entries
   */
  public static String join(String[] collection, String separator,
                            boolean trailing) {
    return join(Arrays.asList(collection), separator, trailing);
  }

  /**
   * Join an array of strings with a separator that appears after every
   * instance in the list -except at the end
   * @param collection strings
   * @param separator separator string
   * @return the list
   */
  public static String joinWithInnerSeparator(String separator,
                                              Object... collection) {
    StringBuilder b = new StringBuilder();
    boolean first = true;

    for (Object o : collection) {
      if (first) {
        first = false;
      } else {
        b.append(separator);
      }
      b.append(o.toString());
      b.append(separator);
    }
    return b.toString();
  }

  public static String mandatoryEnvVariable(String key) {
    String v = System.getenv(key);
    if (v == null) {
      throw new MissingArgException("Missing Environment variable " + key);
    }
    return v;
  }

  public static String appReportToString(ApplicationReport r, String separator) {
    StringBuilder builder = new StringBuilder(512);
    builder.append("application ").append(
      r.getName()).append("/").append(r.getApplicationType());
    builder.append(separator).append(
      "state: ").append(r.getYarnApplicationState());
    builder.append(separator).append("URL: ").append(r.getTrackingUrl());
    builder.append(separator).append("Started ").append(new Date(r.getStartTime()).toGMTString());
    long finishTime = r.getFinishTime();
    if (finishTime>0) {
      builder.append(separator).append("Finished ").append(new Date(finishTime).toGMTString());
    }
    builder.append(separator).append("RPC :").append(r.getHost()).append(':').append(r.getRpcPort());
    String diagnostics = r.getDiagnostics();
    if (!diagnostics.isEmpty()) {
      builder.append(separator).append("Diagnostics :").append(diagnostics);
    }
    return builder.toString();
  }

  /**
   * Merge in one map to another -all entries in the second map are
   * merged into the first -overwriting any duplicate keys.
   * @param first first map -the updated one.
   * @param second the map that is merged in
   * @return the first map
   */
  public static Map<String, String>  mergeMap(Map<String, String> first,
           Map<String, String> second) {
    first.putAll(second);
    return first;
  }

  /**
   * Merge a set of entries into a map. This will take the entryset of
   * a map, or a Hadoop collection itself
   * @param dest destination
   * @param entries entries
   * @return dest -with the entries merged in
   */
  public static Map<String, String> mergeEntries(Map<String, String> dest,
                                                 Iterable<Map.Entry<String, String>> entries) {
    for (Map.Entry<String, String> entry: entries) {
      dest.put(entry.getKey(), entry.getValue());
    }
    return dest;
  }

  /**
   * Generic map merge logic
   * @param first first map
   * @param second second map
   * @param <T1> key type
   * @param <T2> value type
   * @return 'first' merged with the second
   */
  public static <T1, T2> Map<T1, T2>  mergeMaps(Map<T1, T2> first,
           Map<T1, T2> second) {
    first.putAll(second);
    return first;
  }

  /**
   * Generic map merge logic
   * @param first first map
   * @param second second map
   * @param <T1> key type
   * @param <T2> value type
   * @return 'first' merged with the second
   */
  public static <T1, T2> Map<T1, T2> mergeMapsIgnoreDuplicateKeys(Map<T1, T2> first,
                                                                  Map<T1, T2> second) {
    for (Map.Entry<T1, T2> entry : second.entrySet()) {
      T1 key = entry.getKey();
      if (!first.containsKey(key)) {
        first.put(key, entry.getValue());
      }
    }
    return first;
  }

  /**
   * Convert a map to a multi-line string for printing
   * @param map map to stringify
   * @return a string representation of the map
   */
  public static String stringifyMap(Map<String, String> map) {
    StringBuilder builder =new StringBuilder();
    for (Map.Entry<String, String> entry: map.entrySet()) {
      builder.append(entry.getKey())
             .append("=\"")
             .append(entry.getValue())
             .append("\"\n");

    }
    return builder.toString();
  }

  /**
   * Get the int value of a role
   * @param roleMap map of role key->val entries
   * @param key key the key to look for
   * @param defVal default value to use if the key is not in the map
   * @param min min value or -1 for do not check
   * @param max max value or -1 for do not check
   * @return the int value the integer value
   * @throws BadConfigException if the value could not be parsed
   */
  public static int getIntValue(Map<String, String> roleMap,
                         String key,
                         int defVal,
                         int min,
                         int max
                        ) throws BadConfigException {
    String valS = roleMap.get(key);
    return parseAndValidate(key, valS, defVal, min, max);

  }

  /**
   * Parse an int value, replacing it with defval if undefined;
   * @param errorKey key to use in exceptions
   * @param defVal default value to use if the key is not in the map
   * @param min min value or -1 for do not check
   * @param max max value or -1 for do not check
   * @return the int value the integer value
   * @throws BadConfigException if the value could not be parsed
   */
  public static int parseAndValidate(String errorKey,
                                     String valS,
                                     int defVal,
                                     int min, int max) throws
                                                       BadConfigException {
    if (valS == null) {
      valS = Integer.toString(defVal);
    }
    String trim = valS.trim();
    int val;
    try {
      val = Integer.decode(trim);
    } catch (NumberFormatException e) {
      throw new BadConfigException("Failed to parse value of "
                                   + errorKey + ": \"" + trim + "\"");
    }
    if (min >= 0 && val < min) {
      throw new BadConfigException("Value of "
                                   + errorKey + ": " + val + ""
                                   + "is less than the minimum of " + min);
    }
    if (max >= 0 && val > max) {
      throw new BadConfigException("Value of "
                                   + errorKey + ": " + val + ""
                                   + "is more than the maximum of " + max);
    }
    return val;
  }

  public static InetSocketAddress getRmAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_PORT);
  }

  public static InetSocketAddress getRmSchedulerAddress(Configuration conf) {
    return conf.getSocketAddr(YarnConfiguration.RM_SCHEDULER_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_SCHEDULER_ADDRESS,
                              YarnConfiguration.DEFAULT_RM_SCHEDULER_PORT);
  }

  /**
   * probe to see if the RM scheduler is defined
   * @param conf config
   * @return true if the RM scheduler address is set to
   * something other than 0.0.0.0
   */
  public static boolean isRmSchedulerAddressDefined(Configuration conf) {
    InetSocketAddress address = getRmSchedulerAddress(conf);
    return isAddressDefined(address);
  }

  /**
   * probe to see if the address
   * @param address
   * @return true if the scheduler address is set to
   * something other than 0.0.0.0
   */
  public static boolean isAddressDefined(InetSocketAddress address) {
    return !(address.getHostName().equals("0.0.0.0"));
  }

  public static void setRmAddress(Configuration conf, String rmAddr) {
    conf.set(YarnConfiguration.RM_ADDRESS, rmAddr);
  }

  public static void setRmSchedulerAddress(Configuration conf, String rmAddr) {
    conf.set(YarnConfiguration.RM_SCHEDULER_ADDRESS, rmAddr);
  }

  public static boolean hasAppFinished(ApplicationReport report) {
    return report == null ||
           report.getYarnApplicationState().ordinal() >=
           YarnApplicationState.FINISHED.ordinal();
  }

  public static String containerToString(Container container) {
    if (container == null) {
      return "null container";
    }
    return String.format(Locale.ENGLISH,
                         "ContainerID=%s nodeID=%s http=%s priority=%s",
                         container.getId(),
                         container.getNodeId(),
                         container.getNodeHttpAddress(),
                         container.getPriority());
  }

  /**
   * convert an AM report to a string for diagnostics
   * @param report the report
   * @return the string value
   */
  public static String reportToString(ApplicationReport report) {
    if (report == null) {
      return "Null application report";
    }

    return "App " + report.getName() + "/" + report.getApplicationType() +
           "# " +
           report.getApplicationId() + " user " + report.getUser() +
           " is in state " + report.getYarnApplicationState() +
           " RPC: " + report.getHost() + ":" + report.getRpcPort() +
           " URL" + report.getOriginalTrackingUrl();
  }

  /**
   * Convert a YARN URL into a string value of a normal URL
   * @param url URL
   * @return string representatin
   */
  public static String stringify(org.apache.hadoop.yarn.api.records.URL url) {
    StringBuilder builder = new StringBuilder();
    builder.append(url.getScheme()).append("://");
    if (url.getHost() != null) {
      builder.append(url.getHost()).append(":").append(url.getPort());
    }
    builder.append(url.getFile());
    return builder.toString();
  }

  public static int findFreePort(int start, int limit) {
    if (start == 0) {
      //bail out if the default is "dont care"
      return 0;
    }
    int found = 0;
    int port = start;
    int finish = start + limit;
    while (found == 0 && port < finish) {
      if (isPortAvailable(port)) {
        found = port;
      } else {
        port++;
      }
    }
    return found;
  }

  /**
   * See if a port is available for listening on by trying to listen
   * on it and seeing if that works or fails.
   * @param port port to listen to
   * @return true if the port was available for listening on
   */
  public static boolean isPortAvailable(int port) {
    try {
      ServerSocket socket = new ServerSocket(port);
      socket.close();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Build the environment map from a role option map, finding all entries
   * beginning with "env.", adding them to a map of (prefix-removed)
   * env vars
   * @param roleOpts role options. This can be null, meaning the
   * role is undefined
   * @return a possibly empty map of environment variables.
   */
  public static Map<String, String> buildEnvMap(Map<String, String> roleOpts) {
    Map<String, String> env = new HashMap<String, String>();
    if (roleOpts != null) {
      for (Map.Entry<String, String> entry:roleOpts.entrySet()) {
        String key = entry.getKey();
        if (key.startsWith(RoleKeys.ENV_PREFIX)) {
          String envName = key.substring(RoleKeys.ENV_PREFIX.length());
          if (!envName.isEmpty()) {
            env.put(envName,entry.getValue());
          }
        }
      }
    }
    return env;
  }

  /**
   * Apply a set of command line options to a cluster role map
   * @param clusterRoleMap cluster role map to merge onto
   * @param commandOptions command opts
   */
  public static void applyCommandLineRoleOptsToRoleMap(Map<String, Map<String, String>> clusterRoleMap,
                                                       Map<String, Map<String, String>> commandOptions) {
    for (Map.Entry<String, Map<String, String>> entry: commandOptions.entrySet()) {
      String key = entry.getKey();
      Map<String, String> optionMap = entry.getValue();
      Map<String, String> existingMap = clusterRoleMap.get(key);
      if (existingMap == null) {
        existingMap = new HashMap<String, String>();
      }
      log.debug("Overwriting role options with command line values {}",
                stringifyMap(optionMap));
      mergeMap(existingMap, optionMap);
      //set or overwrite the role
      clusterRoleMap.put(key, existingMap);
    }
  }

  /**
   * verify that the supplied cluster name is valid
   * @param clustername cluster name
   * @throws BadCommandArgumentsException if it is invalid
   */
  public static void validateClusterName(String clustername) throws
                                                         BadCommandArgumentsException {
    if (!isClusternameValid(clustername)) {
      throw new BadCommandArgumentsException(
        "Illegal cluster name: " + clustername);
    }
  }

  /**
   * Verify that a Kerberos principal has been set -if not fail
   * with an error message that actually tells you what is missing
   * @param conf configuration to look at
   * @param principal key of principal
   * @throws BadConfigException if the key is not set
   */
  public static void verifyPrincipalSet(Configuration conf,
                                        String principal) throws
                                                           BadConfigException {
    String principalName = conf.get(principal);
    if (principalName == null) {
      throw new BadConfigException("Unset Kerberos principal : %s",
                                   principal);
    }
    log.debug("Kerberos princial {}={}", principal, principalName);
  }

  /**
   * Flag to indicate whether the cluster is in secure mode
   * @param conf configuration to look at
   * @return true if the slider client/service should be in secure mode
   */
  public static boolean isHadoopClusterSecure(Configuration conf) {
    return conf.getBoolean(SliderXmlConfKeys.KEY_SECURITY_ENABLED, false);
  }

  /**
   * Init security if the cluster configuration declares the cluster is secure
   * @param conf configuration to look at
   * @return true if the cluster is secure
   * @throws IOException cluster is secure
   * @throws BadConfigException the configuration/process is invalid
   */
  public static boolean maybeInitSecurity(Configuration conf) throws
                                                              IOException,
                                                              BadConfigException {
    boolean clusterSecure = isHadoopClusterSecure(conf);
    if (clusterSecure) {
      log.debug("Enabling security");
      initProcessSecurity(conf);
    }
    return clusterSecure;
  }

  /**
   * Turn on security. This is setup to only run once.
   * @param conf configuration to build up security
   * @return true if security was initialized in this call
   * @throws IOException IO/Net problems
   * @throws BadConfigException the configuration and system state are inconsistent
   */
  public static boolean initProcessSecurity(Configuration conf) throws
                                                                IOException,
                                                                BadConfigException {

    if (processSecurityAlreadyInitialized.compareAndSet(true, true)) {
      //security is already inited
      return false;
    }

    log.info("JVM initialized into secure mode with kerberos realm {}",
        SliderUtils.getKerberosRealm());
    //this gets UGI to reset its previous world view (i.e simple auth)
    //security
    log.debug("java.security.krb5.realm={}",
              System.getProperty(JAVA_SECURITY_KRB5_REALM, ""));
    log.debug("java.security.krb5.kdc={}",
              System.getProperty(JAVA_SECURITY_KRB5_KDC, ""));
    log.debug("hadoop.security.authentication={}",
        conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION));
    log.debug("hadoop.security.authorization={}",
        conf.get(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION));
    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation authUser = UserGroupInformation.getCurrentUser();
    log.debug("Authenticating as " + authUser.toString());
    log.debug("Login user is {}", UserGroupInformation.getLoginUser());
    if (!UserGroupInformation.isSecurityEnabled()) {
      throw new BadConfigException("Although secure mode is enabled," +
                                   "the application has already set up its user as an insecure entity %s",
                                   authUser);
    }
    if (authUser.getAuthenticationMethod() ==
        UserGroupInformation.AuthenticationMethod.SIMPLE) {
      throw new BadConfigException("Auth User is not Kerberized %s" +
                   " -security has already been set up with the wrong authentication method",
                                   authUser);

    }

    SliderUtils.verifyPrincipalSet(conf, YarnConfiguration.RM_PRINCIPAL);
    SliderUtils.verifyPrincipalSet(conf,
        DFSConfigKeys.DFS_NAMENODE_USER_NAME_KEY);
    return true;
  }

  /**
   * Force an early login: This catches any auth problems early rather than
   * in RPC operations
   * @throws IOException if the login fails
   */
  public static void forceLogin() throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      if (UserGroupInformation.isLoginKeytabBased()) {
        UserGroupInformation.getLoginUser().reloginFromKeytab();
      } else {
        UserGroupInformation.getLoginUser().reloginFromTicketCache();
      }
    }
  }

  /**
   * Submit a JAR containing a specific class and map it
   * @param providerResources provider map to build up
   * @param sliderFileSystem remote fs
   * @param clazz class to look for
   * @param libdir lib directory
   * @param jarName <i>At the destination</i>
   * @return the local resource ref
   * @throws IOException trouble copying to HDFS
   */
  public static LocalResource putJar(Map<String, LocalResource> providerResources,
                              SliderFileSystem sliderFileSystem,
                              Class clazz,
                              Path tempPath,
                              String libdir,
                              String jarName
                             )
    throws IOException, SliderException {
    LocalResource res = sliderFileSystem.submitJarWithClass(
            clazz,
            tempPath,
            libdir,
            jarName);
    providerResources.put(libdir + "/"+ jarName, res);
    return res;
  }

    public static Map<String, Map<String, String>> deepClone(Map<String, Map<String, String>> src) {
    Map<String, Map<String, String>> dest =
      new HashMap<String, Map<String, String>>();
    for (Map.Entry<String, Map<String, String>> entry : src.entrySet()) {
      dest.put(entry.getKey(), stringMapClone(entry.getValue()));
    }
    return dest;
  }

  public static Map<String, String> stringMapClone(Map<String, String> src) {
    Map<String, String> dest =  new HashMap<String, String>();
    return mergeEntries(dest, src.entrySet());
  }

  /**
   * List a directory in the local filesystem
   * @param dir directory
   * @return a listing, one to a line
   */
  public static String listDir(File dir) {
    if (dir == null) {
      return "";
    }
    StringBuilder builder = new StringBuilder();
    String[] confDirEntries = dir.list();
    for (String entry : confDirEntries) {
      builder.append(entry).append("\n");
    }
    return builder.toString();
  }

  /**
   * Create a file:// path from a local file
   * @param file file to point the path
   * @return a new Path
   */
  public static Path createLocalPath(File file) {
    return new Path(file.toURI());
  }

  /**
   * Get the current user -relays to
   * {@link UserGroupInformation#getCurrentUser()}
   * with any Slider-specific post processing and exception handling
   * @return user info
   * @throws IOException on a failure to get the credentials
   */
  public static UserGroupInformation getCurrentUser() throws IOException {

    try {
      UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
      return currentUser;
    } catch (IOException e) {
      log.info("Failed to grt user info", e);
      throw e;
    }
  }

  public static String getKerberosRealm() {
    try {
      return KerberosUtil.getDefaultRealm();
    } catch (Exception e) {
      log.debug("introspection into JVM internals failed", e);
      return "(unknown)";

    }
  }

  /**
   * Register the client resource in
   * {@link SliderKeys#CLIENT_RESOURCE}
   * for Configuration instances.
   *
   * @return true if the resource could be loaded
   */
  public static URL registerClientResource() {
    return ConfigHelper.registerDefaultResource(SliderKeys.CLIENT_RESOURCE);
  }


  /**
   * Attempt to load the slider client resource. If the
   * resource is not on the CP an empty config is returned.
   * @return a config
   */
  public static Configuration loadClientConfigurationResource() {
    return ConfigHelper.loadFromResource(SliderKeys.CLIENT_RESOURCE);
  }

  /**
   * Convert a char sequence to a string.
   * This ensures that comparisions work
   * @param charSequence source
   * @return the string equivalent
   */
  public static String sequenceToString(CharSequence charSequence) {
    StringBuilder stringBuilder = new StringBuilder(charSequence);
    return stringBuilder.toString();
  }

  /**
   * Build up the classpath for execution
   * -behaves very differently on a mini test cluster vs a production
   * production one.
   *
   * @param sliderConfDir relative path to the dir containing slider config
   *                      options to put on the classpath -or null
   * @param libdir directory containing the JAR files
   * @param config the configuration
   * @param usingMiniMRCluster flag to indicate the MiniMR cluster is in use
   * (and hence the current classpath should be used, not anything built up)
   * @return a classpath
   */
  public static ClasspathConstructor buildClasspath(String sliderConfDir,
                                                    String libdir,
                                                    Configuration config,
                                                    boolean usingMiniMRCluster) {

    ClasspathConstructor classpath = new ClasspathConstructor();
    
    // add the runtime classpath needed for tests to work
    if (usingMiniMRCluster) {
      // for mini cluster we pass down the java CP properties
      // and nothing else
      classpath.appendAll(classpath.javaVMClasspath());
    } else {
      classpath.addLibDir("./" + libdir);
      if (sliderConfDir != null) {
        classpath.addClassDirectory(sliderConfDir);
      }
      classpath.addRemoteClasspathEnvVar();
      classpath.appendAll(classpath.yarnApplicationClasspath(config));
    }
    return classpath;
  }

  /**
   * Verify that a path refers to a directory. If not
   * logs the parent dir then throws an exception
   * @param dir the directory
   * @param errorlog log for output on an error
   * @throws FileNotFoundException if it is not a directory
   */
  public static void verifyIsDir(File dir, Logger errorlog) throws FileNotFoundException {
    if (!dir.exists()) {
      errorlog.warn("contents of {}: {}", dir,
                    listDir(dir.getParentFile()));
      throw new FileNotFoundException(dir.toString());
    }
    if (!dir.isDirectory()) {
      errorlog.info("contents of {}: {}", dir,
                    listDir(dir.getParentFile()));
      throw new FileNotFoundException(
        "Not a directory: " + dir);
    }
  }

  /**
   * Verify that a file exists
   * @param file file
   * @param errorlog log for output on an error
   * @throws FileNotFoundException
   */
  public static void verifyFileExists(File file, Logger errorlog) throws FileNotFoundException {
    if (!file.exists()) {
      errorlog.warn("contents of {}: {}", file,
                    listDir(file.getParentFile()));
      throw new FileNotFoundException(file.toString());
    }
    if (!file.isFile()) {
      throw new FileNotFoundException("Not a file: " + file.toString());
    }
  }

  /**
   * verify that a config option is set
   * @param configuration config
   * @param key key
   * @return the value, in case it needs to be verified too
   * @throws BadConfigException if the key is missing
   */
  public static String verifyOptionSet(Configuration configuration, String key,
                                       boolean allowEmpty) throws BadConfigException {
    String val = configuration.get(key);
    if (val == null) {
      throw new BadConfigException(
        "Required configuration option \"%s\" not defined ", key);
    }
    if (!allowEmpty && val.isEmpty()) {
      throw new BadConfigException(
        "Configuration option \"%s\" must not be empty", key);
    }
    return val;
  }

  /**
   * Verify that a keytab property is defined and refers to a non-empty file
   *
   * @param siteConf configuration
   * @param prop property to look for
   * @return the file referenced
   * @throws BadConfigException on a failure
   */
  public static File verifyKeytabExists(Configuration siteConf, String prop) throws
                                                                      BadConfigException {
    String keytab = siteConf.get(prop);
    if (keytab == null) {
      throw new BadConfigException("Missing keytab property %s",
                                   prop);

    }
    File keytabFile = new File(keytab);
    if (!keytabFile.exists()) {
      throw new BadConfigException("Missing keytab file %s defined in %s",
                                   keytabFile,
                                   prop);
    }
    if (keytabFile.length() == 0 || !keytabFile.isFile()) {
      throw new BadConfigException("Invalid keytab file %s defined in %s",
                                   keytabFile,
                                   prop);
    }
    return keytabFile;
  }

  /**
   * Convert an epoch time to a GMT time. This
   * uses the deprecated Date.toString() operation,
   * so is in one place to reduce the number of deprecation warnings.
   * @param time timestamp
   * @return string value as ISO-9601
   */
  @SuppressWarnings({"CallToDateToString", "deprecation"})
  public static String toGMTString(long time) {
    return new Date(time).toGMTString();
  }

  /**
   * Add the cluster build information; this will include Hadoop details too
   * @param cd cluster
   * @param prefix prefix for the build info
   */
  public static void addBuildInfo(Map<String, String> info, String prefix) {

    Properties props = SliderVersionInfo.loadVersionProperties();
    info.put(prefix + "." + SliderVersionInfo.APP_BUILD_INFO, props.getProperty(
      SliderVersionInfo.APP_BUILD_INFO));
    info.put(prefix + "." + SliderVersionInfo.HADOOP_BUILD_INFO,
             props.getProperty(SliderVersionInfo.HADOOP_BUILD_INFO));

    info.put(prefix + "." + SliderVersionInfo.HADOOP_DEPLOYED_INFO,
             VersionInfo.getBranch() + " @" + VersionInfo.getSrcChecksum());
  }

  /**
   * Set the time for an information (human, machine) timestamp pair of fields.
   * The human time is the time in millis converted via the {@link Date} class.
   * @param info info fields
   * @param keyHumanTime name of human time key
   * @param keyMachineTime name of machine time
   * @param time timestamp
   */
  public static void setInfoTime(Map info,
                                 String keyHumanTime,
                          String keyMachineTime,
                          long time) {
    info.put(keyHumanTime, SliderUtils.toGMTString(time));
    info.put(keyMachineTime, Long.toString(time));
  }

  public static Path extractImagePath(CoreFileSystem fs,  MapOperations internalOptions) throws
      SliderException,
                                                                                         IOException {
    Path imagePath;
    String imagePathOption =
      internalOptions.get(OptionKeys.INTERNAL_APPLICATION_IMAGE_PATH);
    String appHomeOption = internalOptions.get(OptionKeys.INTERNAL_APPLICATION_HOME);
    if (!isUnset(imagePathOption)) {
      imagePath = fs.createPathThatMustExist(imagePathOption);
    } else {
      imagePath = null;
      if (isUnset(appHomeOption)) {
        throw new BadClusterStateException(ErrorStrings.E_NO_IMAGE_OR_HOME_DIR_SPECIFIED);
      }
    }
    return imagePath;
  }
  
  /**
   * trigger a  JVM halt with no clean shutdown at all
   * @param status status code for exit
   * @param text text message
   * @param delay delay in millis
   * @return the timer (assuming the JVM hasn't halted yet)
   *
   */
  public static Timer haltAM(int status, String text, int delay) {

    Timer timer = new Timer("halt timer", false);
    timer.schedule(new DelayedHalt(status, text), delay);
    return timer;
  }

  public static String propertiesToString(Properties props) {
    TreeSet<String> keys = new TreeSet<String>(props.stringPropertyNames());
    StringBuilder builder = new StringBuilder();
    for (String key : keys) {
      builder.append(key)
             .append("=")
             .append(props.getProperty(key))
             .append("\n");
    }
    return builder.toString();
  }

  /**
   * Callable for async/scheduled halt
   */
  public static class DelayedHalt extends TimerTask {
    private final int status;
    private final String text;

    public DelayedHalt(int status, String text) {
      this.status = status;
      this.text = text;
    }

    @Override
    public void run() {
      try {
        ExitUtil.halt(status, text);
        //this should never be reached
      } catch (ExitUtil.HaltException e) {
        log.info("Halt failed");
      }
    }
  }

  /**
   * This wrapps ApplicationReports and generates a string version
   * iff the toString() operator is invoked
   */
  public static class OnDemandReportStringifier {
    private final ApplicationReport report;

    public OnDemandReportStringifier(ApplicationReport report) {
      this.report = report;
    }

    @Override
    public String toString() {
      return appReportToString(report, "\n");
    }
  }

}
