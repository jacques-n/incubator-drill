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

import org.apache.drill.BaseInjecting;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.ForemanException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestFailures extends BaseInjecting {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestFailures.class);

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(5000);

  @Test
  public void foremanConstructorRuntime(){
    injectRuntimeException(QueryType.SIMPLE, Foreman.class, "constructor");
  }

  @Test
  public void foremanConstructorError(){
    injectError(QueryType.SIMPLE, Foreman.class, "constructor");
  }

  @Test
  public void foremanRun1RE(){
    injectRuntimeException(QueryType.SIMPLE, Foreman.class, "run1");
  }

  @Test
  public void foremanRun1Error(){
    injectError(QueryType.SIMPLE, Foreman.class, "run1");
  }

  @Test
  public void foremanRun2RE(){
    injectRuntimeException(QueryType.SIMPLE, Foreman.class, "run2");
  }

  @Test
  public void foremanRun2Error(){
    injectError(QueryType.SIMPLE, Foreman.class, "run2");
  }

  @Test
  public void foremanRun2Foreman(){
    injectException(QueryType.SIMPLE, Foreman.class, "run2", ForemanException.class);
  }


}
