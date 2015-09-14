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
package org.apache.drill.exec;

import java.lang.reflect.Modifier;

import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtMethod;
import javassist.CtNewMethod;

import org.apache.drill.exec.metrics.DrillMetrics;
import org.apache.drill.test.DrillTest;
import org.junit.After;

public class ExecTest extends DrillTest {

  /**
   * Makes Guava stopwatch look like the old version for compatibility with hbase-server (for test purposes).
   */
  private static void patchStopwatch() {

    try{
      ClassPool cp = ClassPool.getDefault();
      CtClass cc = cp.get("com.google.common.base.Stopwatch");

      // Expose the constructor for Stopwatch for old libraries who use the pattern new Stopwatch().start().
      for (CtConstructor c : cc.getConstructors()) {
        if (!Modifier.isStatic(c.getModifiers())) {
          System.out.println("Exposed " + c);
          c.setModifiers(Modifier.PUBLIC);
        }
      }

      // Add back the Stopwatch.elapsedMillis() method for old consumers.
      CtMethod newmethod = CtNewMethod.make(
          "public long elapsedMillis() { return elapsed(java.util.concurrent.TimeUnit.MILLISECONDS); }", cc);
      cc.addMethod(newmethod);

      // Load the modified class instead of the original.
      cc.toClass();
    } catch (Exception e) {
      System.err.println("Failure while patching Stopwatch. Exiting.\n");
      e.printStackTrace();
      System.err.flush();
      System.exit(-100);
    }
  }

  static {
    patchStopwatch();
  }

  @After
  public void clear(){
    // TODO:  (Re DRILL-1735) Check whether still needed now that
    // BootstrapContext.close() resets the metrics.
    DrillMetrics.resetMetrics();
  }

}
