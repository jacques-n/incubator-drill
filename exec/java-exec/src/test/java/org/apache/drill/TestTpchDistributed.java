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

import org.junit.Ignore;
import org.junit.Test;

public class TestTpchDistributed extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTpchDistributed.class);

  private void testDistributed(String fileName) throws Exception{
    String query = getFile(fileName);
    test("alter session set `planner.slice_target` = 10; " + query);
  }

  @Test
  public void tpch01() throws Exception{
    testDistributed("queries/tpch/01.sql");
  }

  @Test
  public void testC() throws Exception {
    test(" "
        + "select n.dma_name\n" +
        "     , (case when fm.segmentid=39730 then 'bonefish'\n" +
        "             when fm.segmentid=31050 then 'cheesecake'\n" +
        "             when fm.segmentid=33980 or fm.segmentid=52836 then 'pfchangs'\n" +
        "             when fm.segmentid=33990 then 'pandaexpress'\n" +
        "             when fm.segmentid=39505 then 'peiwei'\n" +
        "             when fm.segmentid=40080 then 'whichwich'\n" +
        "             else 'other' end\n" +
        "       ) as advertiser\n" +
        "     , sum(case when fm.weekstartdt between to_date('2013-01-03', 'yyyy-mm-dd') and to_date('2013-03-28', 'yyyy-mm-dd') then fm.amount else cast(0 as decimal(19,6)) end) as _2013q1spend\n" +
        "     , sum(case when fm.weekstartdt between to_date('2013-01-03', 'yyyy-mm-dd') and to_date('2013-03-28', 'yyyy-mm-dd') then fm.trips else cast(0 as int) end) as _2013q1trips\n" +
        "     , sum(case when fm.weekstartdt between to_date('2014-01-02', 'yyyy-mm-dd') and to_date('2014-03-27', 'yyyy-mm-dd') then fm.amount else cast(0 as decimal(19,6)) end) as _2014q1spend\n" +
        "     , sum(case when fm.weekstartdt between to_date('2014-01-02', 'yyyy-mm-dd') and to_date('2014-03-27', 'yyyy-mm-dd') then fm.trips else cast(0 as int) end) as _2014q1trips\n" +
        "  from dfs.`/src/data/card/cdw_vft_merchant` fm\n" +
        "  join dfs.`/src/data/card/temp_nielsen_dma_official_060314` n on lower(cast(fm.zipid as varchar(5))) = lower(n.zipcode)\n" +
        " where fm.segmentid in (33990,39505,40080,31050,33980,52836,39730)\n" +
        "   and fm.weekstartdt between to_date('2013-01-03', 'yyyy-mm-dd') and to_date('2014-03-27', 'yyyy-mm-dd')\n" +
        "   and fm.institutionid in (2726, 2252, 2755)\n" +
        " group by n.dma_name\n" +
        "     , (case when fm.segmentid=39730 then 'bonefish'\n" +
        "             when fm.segmentid=31050 then 'cheesecake'\n" +
        "             when fm.segmentid=33980 or fm.segmentid=52836 then 'pfchangs'\n" +
        "             when fm.segmentid=33990 then 'pandaexpress'\n" +
        "             when fm.segmentid=39505 then 'peiwei'\n" +
        "             when fm.segmentid=40080 then 'whichwich'\n" +
        "             else 'other' end\n" +
        "       )\n" +
        ";"
        + "");
  }

  @Test
  @Ignore // DRILL-512
  public void tpch02() throws Exception{
    testDistributed("queries/tpch/02.sql");
  }

  @Test
  public void tpch03() throws Exception{
    testDistributed("queries/tpch/03.sql");
  }

  @Test
  public void tpch04() throws Exception{
    testDistributed("queries/tpch/04.sql");
  }

  @Test
  public void tpch05() throws Exception{
    testDistributed("queries/tpch/05.sql");
  }

  @Test
  public void tpch06() throws Exception{
    testDistributed("queries/tpch/06.sql");
  }

  @Test
  public void tpch07() throws Exception{
    testDistributed("queries/tpch/07.sql");
  }

  @Test
  public void tpch08() throws Exception{
    testDistributed("queries/tpch/08.sql");
  }

  @Test
  public void tpch09() throws Exception{
    testDistributed("queries/tpch/09.sql");
  }

  @Test
  public void tpch10() throws Exception{
    testDistributed("queries/tpch/10.sql");
  }

  @Test
  @Ignore // cartesion problem
  public void tpch11() throws Exception{
    testDistributed("queries/tpch/11.sql");
  }

  @Test
  public void tpch12() throws Exception{
    testDistributed("queries/tpch/12.sql");
  }

  @Test
  public void tpch13() throws Exception{
    testDistributed("queries/tpch/13.sql");
  }

  @Test
  public void tpch14() throws Exception{
    testDistributed("queries/tpch/14.sql");
  }

  @Test
  @Ignore // non-equality join
  public void tpch15() throws Exception{
    testDistributed("queries/tpch/15.sql");
  }

  @Test
  public void tpch16() throws Exception{
    testDistributed("queries/tpch/16.sql");
  }

  @Test
  @Ignore // non-equality join
  public void tpch17() throws Exception{
    testDistributed("queries/tpch/17.sql");
  }

  @Test
  public void tpch18() throws Exception{
    testDistributed("queries/tpch/18.sql");
  }

  @Test
  @Ignore // non-equality join
  public void tpch19() throws Exception{
    testDistributed("queries/tpch/19.sql");
  }

  @Test
  public void tpch19_1() throws Exception{
    testDistributed("queries/tpch/19_1.sql");
  }

  @Test
  public void tpch20() throws Exception{
    testDistributed("queries/tpch/20.sql");
  }

  @Test
  @Ignore
  public void tpch21() throws Exception{
    testDistributed("queries/tpch/21.sql");
  }

  @Test
  @Ignore // DRILL-518
  public void tpch22() throws Exception{
    testDistributed("queries/tpch/22.sql");
  }

}
