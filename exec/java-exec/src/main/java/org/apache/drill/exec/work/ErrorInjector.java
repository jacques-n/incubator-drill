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
package org.apache.drill.exec.work;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.drill.exec.util.AssertionUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public final class ErrorInjector {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ErrorInjector.class);

  public static final String INJECTION_MESSAGE_CHECK = "<<INJECTED>>";

  public static ErrorInjector INSTANCE = new ErrorInjector();
  private static volatile RealInjector activeInjector;
  private static volatile AtomicBoolean injected = new AtomicBoolean(false);
  private static volatile boolean single = false;
  private static final NoOpInjector NO_OP = new NoOpInjector();
  private static final AtomicLong firings = new AtomicLong(0);

  private ErrorInjector(){}


  @VisibleForTesting
  public static void clear(){
    activeInjector = null;
    injected.set(false);
    single = false;
    firings.set(0);
  }

  @VisibleForTesting
  public static void enableInjection(Class<?> clazz, String desc, Class<? extends Throwable> exceptionClass){
    enableInjection(clazz.getName(), desc, exceptionClass);
  }

  @VisibleForTesting
  public static void enableInjection(String className, String desc, Class<? extends Throwable> exceptionClass){
    Preconditions.checkArgument(activeInjector == null);
    activeInjector = new RealInjector(className, desc, exceptionClass, false);
    single = false;
  }

  @VisibleForTesting
  public static void enableInjectionOnce(String className, String desc, Class<? extends Throwable> exceptionClass){
    Preconditions.checkArgument(activeInjector == null);
    activeInjector = new RealInjector(className, desc, exceptionClass, true);
    single = true;
  }

  public static boolean wasInjected(){
    return firings.get() > 0;
  }

  public static Injector inject(String desc){
    if(AssertionUtil.ASSERT_ENABLED && activeInjector != null){
      if(activeInjector.desc.equals(desc) && activeInjector.callingClass.equals(getCallerClassName(3))){
        if(!single || injected.compareAndSet(false, true)){
          firings.incrementAndGet();
          activeInjector.throwIfUncaught();
          return activeInjector;
        }
      }
    }
    return NO_OP;
  }

  public static interface Injector {
    /**
     * Add a caught exception to be thrown.  In this case, the exception will be executed using a method with a single message.
     * @param excep
     * @return
     * @throws V
     */
    public <V extends Throwable> Injector t(Class<V> excep) throws V;
  }


  private static class NoOpInjector implements Injector {
    @Override
    public <V extends Throwable> Injector t(Class<V> excep) throws V {
      return this;
    }

  }

  public static class RealInjector implements Injector {
    final AtomicBoolean injected = new AtomicBoolean(false);
    String callingClass;
    String desc;
    Class<?> exceptionClass;
    boolean single;

    public RealInjector(String callingClass, String desc, Class<?> exceptionClass, boolean single) {
      super();
      this.callingClass = callingClass;
      this.desc = desc;
      this.exceptionClass = exceptionClass;
    }

    private void throwIfUncaught(){
      if(RuntimeException.class.isAssignableFrom(exceptionClass)){
        throw (RuntimeException) newException(exceptionClass, "<<INJECTED>>");
      }

      if(Error.class.isAssignableFrom(exceptionClass)){
        throw (Error) newException(exceptionClass, "<<INJECTED>>");
      }
    }

    /**
     * Add a caught exception to be thrown.  In this case, the exception will be executed using a method with a single message.
     * @param excep
     * @return
     * @throws V
     */
    public <V extends Throwable> Injector t(Class<V> excep) throws V{
      if(excep == exceptionClass){
        if(!single || injected.compareAndSet(false, true)){
          throw newException(excep, "<<INJECTED>>");
        }
      }
      return this;
    }
  }


  private static <V> V newException(Class<V> eClazz, String message){
    try {
      return eClazz.getDeclaredConstructor(String.class).newInstance(message);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException
        | NoSuchMethodException | SecurityException e) {
      throw new RuntimeException("Exception construction failed.", e);
    }
  }

  @SuppressWarnings("restriction")
  public static String getCallerClassName(int callStackDepth) {
      return sun.reflect.Reflection.getCallerClass(callStackDepth).getName();
  }


}
