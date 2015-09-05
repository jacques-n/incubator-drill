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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.config.CommonConstants;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;

public class PathScanner {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PathScanner.class);

  private static final SubTypesScanner subTypeScanner = new SubTypesScanner();
  static volatile Collection<URL> MARKED_PATHS;

  private static Map<ScanKey, Object> SCAN_CACHE = new ConcurrentHashMap<>(64, 0.5f, 2);

  /**
   * @param  scanPackages  note:  not currently used
   */
  public static <T> Class<?>[] scanForImplementationsArr(final Class<T> baseClass,
                                                         final List<String> scanPackages) {
    Collection<Class<? extends T>> imps = scanForImplementations(baseClass, scanPackages);
    Class<?>[] arr = imps.toArray(new Class<?>[imps.size()]);
    return arr;
  }

  private static class ScanKey {
    final Class<?> baseClass;
    final List<String> scanPackages;

    public ScanKey(Class<?> baseClass, List<String> scanPackages) {
      super();
      this.baseClass = baseClass;
      this.scanPackages = scanPackages;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((baseClass == null) ? 0 : baseClass.hashCode());
      result = prime * result + ((scanPackages == null) ? 0 : scanPackages.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ScanKey other = (ScanKey) obj;
      if (baseClass == null) {
        if (other.baseClass != null) {
          return false;
        }
      } else if (!baseClass.equals(other.baseClass)) {
        return false;
      }
      if (scanPackages == null) {
        if (other.scanPackages != null) {
          return false;
        }
      } else if (!scanPackages.equals(other.scanPackages)) {
        return false;
      }
      return true;
    }


  }


  /**
   * @param  scanPackages  note:  not currently used
   */
  public static <T> Set<Class<? extends T>> scanForImplementations(final Class<T> baseClass,
      final List<String> scanPackages) {

    final ScanKey key = new ScanKey(baseClass, scanPackages);
    Set<Class<? extends T>> classes = (Set<Class<? extends T>>) SCAN_CACHE.get(key);

    // check cache first.
    if (classes != null) {
      return classes;
    }

    final Stopwatch w = new Stopwatch().start();
    int count = 0;

    try {

      FilterBuilder filter = new FilterBuilder();
      for(String pack : scanPackages){
        filter.include(FilterBuilder.prefix(pack));
      }

      ConfigurationBuilder conf = new ConfigurationBuilder()
        .setUrls(getMarkedPaths())
        .filterInputsBy(filter)
          .setScanners(subTypeScanner);

      Reflections reflect = new Reflections(conf);
      classes = reflect.getSubTypesOf(baseClass);

      for (Iterator<Class<? extends T>> i = classes.iterator(); i.hasNext();) {
        final Class<? extends T> c = i.next();
        assert baseClass.isAssignableFrom(c);
        if (Modifier.isAbstract(c.getModifiers())) {
          i.remove();
        }
      }
      count = classes.size();
      SCAN_CACHE.put(key, classes);

      return classes;
    } finally {
      if (logger.isInfoEnabled()) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Scanning for %s took %dms and found %d implmentations.", baseClass.getName(),
            w.elapsed(TimeUnit.MILLISECONDS), count));
        if (classes != null) {
          for (Class<?> c : classes) {
            sb.append("\n\t-");
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
