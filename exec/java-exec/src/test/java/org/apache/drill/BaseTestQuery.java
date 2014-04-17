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
package org.apache.drill;

import java.io.IOException;
import java.net.URL;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.common.util.TestTools.TestLogReporter;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.client.QuerySubmitter;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.server.RemoteServiceSet;
import org.apache.drill.exec.util.VectorUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

public class BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseTestQuery.class);

  // make it static so we can use after class
  static final TestLogReporter LOG_OUTCOME = TestTools.getTestLogReporter(logger);

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(20000);
  @Rule public final TestLogReporter logOutcome = LOG_OUTCOME;

  @AfterClass
  public static void letLogsCatchUp() throws InterruptedException{
    LOG_OUTCOME.sleepIfFailure();
  }

  public final TestRule resetWatcher = new TestWatcher() {
    @Override
    protected void failed(Throwable e, Description description) {
      try {
        resetClientAndBit();
      } catch (Exception e1) {
        throw new RuntimeException("Failure while resetting client.", e1);
      }
    }
  };

  static DrillClient client;
  static Drillbit bit;
  static RemoteServiceSet serviceSet;
  static DrillConfig config;
  static QuerySubmitter submitter = new QuerySubmitter();

  static void resetClientAndBit() throws Exception{
    closeClient();
    openClient();
  }

  @BeforeClass
  public static void openClient() throws Exception{
    config = DrillConfig.create();
    serviceSet = RemoteServiceSet.getLocalServiceSet();
    bit = new Drillbit(config, serviceSet);
    bit.run();
    client = new DrillClient(config, serviceSet.getCoordinator());
    client.connect();
  }

  @AfterClass
  public static void closeClient() throws IOException{
    if(client != null) client.close();
    if(bit != null) bit.close();
    if(serviceSet != null) serviceSet.close();
  }



  protected void test(String sql) throws Exception{
    sql = sql.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    submitter.submitQuery(client, sql, "sql", "tsv", VectorUtil.DEFAULT_COLUMN_WIDTH);
  }

  protected void testLogical(String logical) throws Exception{
    logical = logical.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    submitter.submitQuery(client, logical, "logical", "tsv", VectorUtil.DEFAULT_COLUMN_WIDTH);
  }

  protected void testPhysical(String physical) throws Exception{
    physical = physical.replace("[WORKING_PATH]", TestTools.getWorkingPath());
    submitter.submitQuery(client, physical, "physical", "tsv", VectorUtil.DEFAULT_COLUMN_WIDTH);
  }

  protected void testPhysicalFromFile(String file) throws Exception{
    testPhysical(getFile(file));
  }
  protected void testLogicalFromFile(String file) throws Exception{
    testLogical(getFile(file));
  }
  protected void testSqlFromFile(String file) throws Exception{
    test(getFile(file));
  }


  protected String getFile(String resource) throws IOException{
    URL url = Resources.getResource(resource);
    if(url == null){
      throw new IOException(String.format("Unable to find path %s.", resource));
    }
    return Resources.toString(url, Charsets.UTF_8);
  }
}
