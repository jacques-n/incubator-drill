/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.common.util;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.config.DrillConfig;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class PathScanner {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PathScanner.class);

  private static final SubTypesScanner subTypeScanner = new SubTypesScanner();
  private static final Object SYNC = new Object();
  private static volatile Collection<URL> MARKED_PATHS;
  private static volatile Set<String> PREFIXES;
  private static volatile Reflections REFLECTIONS;

  /**
   * Note that we're taking a static reference to a local config object. This is ugly but helps us avoid recurrent
   * scanning in testing and should have no impact on production (since only one DrillConfig is ever used in
   * production).
   *
   * @param config
   */
  private static void init(DrillConfig config){
    if(REFLECTIONS == null){
      synchronized(SYNC){
        if(REFLECTIONS != null){
          return;
        }
        final Stopwatch w = new Stopwatch().start();

        PREFIXES = ImmutableSet.copyOf(CommonConstants.PACKAGES_EXCLUDING_FUNCTIONS);
        FilterBuilder filter = new FilterBuilder();
        for (String path : PREFIXES) {
          for (String pkg : config.getStringList(path)) {
            filter.include(FilterBuilder.prefix(pkg));
          }
        }

        ConfigurationBuilder conf = new ConfigurationBuilder()
            .setUrls(getMarkedPaths())
            .filterInputsBy(filter)
            .setScanners(subTypeScanner);

        REFLECTIONS = new Reflections(conf);
        logger.info("Initialized classpath cache in {}ms.", w.stop().elapsed(TimeUnit.MILLISECONDS));

      }
    }
  }
  /**
   * @param scanPackages
   *          note: not currently used
   */
  public static <T> Set<Class<? extends T>> findImplementations(
      final Class<T> baseClass,
      final DrillConfig config,
      final String configurationString
      ) {
    init(config);

    // make sure a cache includes all the scan prefixes.
    Preconditions.checkArgument(PREFIXES.contains(configurationString));

    final Stopwatch w = new Stopwatch().start();
    int count = 0;

    Set<Class<? extends T>> classes = null;

    try {

      synchronized (SYNC) {
        classes = REFLECTIONS.getSubTypesOf(baseClass);
      }

      for (Iterator<Class<? extends T>> i = classes.iterator(); i.hasNext();) {
        final Class<? extends T> c = i.next();
        assert baseClass.isAssignableFrom(c);
        if (Modifier.isAbstract(c.getModifiers())) {
          i.remove();
        }
      }
      count = classes.size();

      return classes;
    } finally {
      if (logger.isInfoEnabled()) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Scanning for %s took %dms and found %d implmentations.", baseClass.getName(),
            w.elapsed(TimeUnit.MILLISECONDS), count));
        if (classes != null) {
          for (Class<?> c : classes) {
            sb.append("\n\t- ");
            sb.append(c.getName());
          }
        }
        logger.info(sb.toString());
      }
    }
  }

  private static Collection<URL> getMarkedPaths() {
    if (MARKED_PATHS != null) {
      return MARKED_PATHS;
    }

    // this could race and overwrite but that shouldn't be an issue.
    MARKED_PATHS = forResource(CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME, true);
    return MARKED_PATHS;
  }

  public static Collection<URL> getConfigURLs() {
    return forResource(CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME, false);
  }

  /**
   * Gets URLs of any classpath resources with given resource pathname.
   *
   * @param  resourcePathname  resource pathname of classpath resource instances
   *           to scan for (relative to specified class loaders' classpath roots)
   * @param  returnRootPathname  whether to collect classpath root portion of
   *           URL for each resource instead of full URL of each resource
   * @param  classLoaders  set of class loaders in which to look up resource;
   *           none (empty array) to specify to use current thread's context
   *           class loader and {@link Reflections}'s class loader
   * @returns  ...; empty set if none
   */
  public static Set<URL> forResource(final String resourcePathname,
                                     final boolean returnRootPathname,
                                     final ClassLoader... classLoaders) {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("Scanning classpath for resources with pathname \"%s\".", resourcePathname));
    final Set<URL> resultUrlSet = Sets.newHashSet();

    final ClassLoader[] netLoaders = ClasspathHelper.classLoaders(classLoaders);
    for (ClassLoader classLoader : netLoaders) {
      try {
        final Enumeration<URL> resourceUrls =
            classLoader.getResources(resourcePathname);
        while (resourceUrls.hasMoreElements()) {
          final URL resourceUrl = resourceUrls.nextElement();
          logger.trace( "- found a(n) {} at {}.", resourcePathname, resourceUrl );

          int index = resourceUrl.toExternalForm().lastIndexOf(resourcePathname);
          if (index != -1 && returnRootPathname) {
            final URL classpathRootUrl =
                new URL(resourceUrl.toExternalForm().substring(0, index));
            resultUrlSet.add(classpathRootUrl);
            sb.append(String.format("\n\t- found resource's classpath root URL %s", classpathRootUrl));
          } else {
            resultUrlSet.add(resourceUrl);
            sb.append(String.format("\n\t- found resource's classpath root URL %s", resourceUrl));
          }
        }
      } catch (IOException e) {
        logger.error("Error scanning for resources named " + resourcePathname, e);
      }

    }

    logger.debug(sb.toString());

    return resultUrlSet;
  }

}
