package org.apache.drill.exec.physical.impl.broadcastsender;

import org.apache.drill.BaseTestQuery;
import org.junit.Test;

public class TestBroadcast extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestBroadcast.class);

  String broadcastQuery = "select * from "
      + "dfs.`${WORKING_PATH}/src/test/resources/broadcast/sales` s "
      + "INNER JOIN "
      + "dfs.`${WORKING_PATH}/src/test/resources/broadcast/customer` c "
      + "ON s.id = c.id";

  @Test
  public void plansWithBroadcast() throws Exception {
    testNoResult("alter session set `planner.slice_target` = 1");
    test("explain plan for " + broadcastQuery);
  }

  @Test
  public void broadcastExecuteWorks() throws Exception {
    testNoResult("alter session set `planner.slice_target` = 1");
    test(broadcastQuery);
    Thread.sleep(1000*2);
  }
}
