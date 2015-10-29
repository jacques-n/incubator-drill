package org.apache.drill.exec.store.jdbc;

import org.apache.drill.BaseTestQuery;
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
public class TestCommercial extends BaseTestQuery {


  @Test
  public void oracle() throws Exception {
    test("ALTER SESSION SET `exec.errors.verbose` = true;");
    test("use oracle;");
    test("show databases;");
    test("show tables;");

    String[] dbs = { "APPQOSSYS", "CTXSYS", "DBSNMP", "DIP", "OUTLN", "RDSADMIN", "ROOT" };
    for (String db : dbs) {
      test("use oracle.`" + db + "`;");
      test("show tables;");
    }
    // test("select * from RDS_CONFIGURATION;");
    test("use oracle.APPQOSSYS");
    // test("select * from ")
    test("select * from oracle.RDSADMIN.RDS_CONFIGURATION");
    test("select * from oracle.APPQOSSYS.WLM_CLASSIFIER_PLAN");
    test("use oracle.APPQOSSYS;");
    test("select * from WLM_CLASSIFIER_PLAN");
    test("use oracle.DBSNMP;");


  }

  @Test
  public void mysql() throws Exception {
    test("ALTER SESSION SET `exec.errors.verbose` = true;");
    test("use mysql;");
    test("show databases;");
    test("show tables;");
  }

  @Test
  public void postgres() throws Exception {
    test("ALTER SESSION SET `exec.errors.verbose` = true;");
    // test("use postgres;");
    // test("show databases;");
    // test("show tables;");
    test("use customers.customers;");
    test("select * from test_table;");
  }

  @Test
  public void mssql() throws Exception {
    test("ALTER SESSION SET `exec.errors.verbose` = true;");
    test("use mssql;");
    test("show databases;");
    test("show tables;");
    test("select * from mssql.dbo.backupfile;");
  }

}
