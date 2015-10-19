package org.apache.drill.exec.store.jdbc;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.PlanTestBase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
public class TestPhoenixPlugin extends PlanTestBase {


  @BeforeClass
  public static void setupDefaultTestCluster() throws Exception {
    // BaseHBaseManagedTimeIT.doSetup();
    BaseTestQuery.setupDefaultTestCluster();
  }

  @AfterClass
  public static void shutdownDb() throws Exception {
    // BaseHBaseManagedTimeIT.doTeardown();
  }

  @Test
  public void firstTest() throws Exception {
    test("explain plan for select * from PHOENIX.A.BEER where e1 >= 1");
    test("select * from PHOENIX.A.BEER where e1 >= 1");
    test("explain plan for select b from PHOENIX.A.BEER where e1 >= 1");
    test("select b from PHOENIX.A.BEER where e1 >= 1");
    test("explain plan for select count(b) from PHOENIX.A.BEER");
    test("select count(b) from PHOENIX.A.BEER");
    test("explain plan for select e1, count(*) from PHOENIX.A.BEER group by e1");
    test("select e2, count(*) from PHOENIX.A.BEER group by e2");
    test("explain plan for select e1, e2, count(b) from PHOENIX.A.BEER group by e1, e2");
    test("select e1, e2, count(b) from PHOENIX.A.BEER group by e1, e2");
  }
}
