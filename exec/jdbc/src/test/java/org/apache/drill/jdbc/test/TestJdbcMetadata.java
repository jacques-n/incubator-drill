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
package org.apache.drill.jdbc.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.Test;


public class TestJdbcMetadata extends JdbcTestQueryBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestJdbcMetadata.class);


  @Test
  public void catalogs() throws Exception{
    this.testAction(new JdbcAction(){
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getCatalogs();
      }
    }, 1);
  }

  @Test
  public void allSchemas() throws Exception{
    this.testAction(new JdbcAction(){
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getSchemas();
      }
    }, 9);
  }

  @Test
  public void schemasWithConditions() throws Exception{
    this.testAction(new JdbcAction(){
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getSchemas("DRILL", "%fs%");
      }
    }, 6);
  }

  @Test
  public void allTables() throws Exception{
    this.testAction(new JdbcAction(){
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getTables(null, null, null, null);
      }
    }, 7);
  }

  @Test
  public void tablesWithConditions() throws Exception{
    this.testAction(new JdbcAction(){
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getTables("DRILL", "sys", "opt%", new String[]{"TABLE", "VIEW"});
      }
    }, 1);
  }

  @Test
  public void allColumns() throws Exception{
    this.testAction(new JdbcAction(){
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getColumns(null, null, null, null);
      }
    }, 38);
  }

  @Test
  public void columnsWithConditions() throws Exception{
    this.testAction(new JdbcAction(){
      public ResultSet getResult(Connection c) throws SQLException {
        return c.getMetaData().getColumns("DRILL", "sys", "opt%", "%ame");
      }
    }, 1);
  }
}
