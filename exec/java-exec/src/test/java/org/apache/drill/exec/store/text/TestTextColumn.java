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
package org.apache.drill.exec.store.text;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import org.apache.drill.exec.store.easy.text.compliant.CompliantTextRecordReader;
import org.apache.drill.exec.store.mock.MockSubScanPOP;
import org.apache.drill.exec.store.schedule.BlockMapBuilder;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.Test;

import com.beust.jcommander.internal.Lists;
import com.codahale.metrics.MetricRegistry;

public class TestTextColumn extends BaseTestQuery{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestTextColumn.class);

  @Test
  public void testCsvColumnSelection() throws Exception{
    test("select columns[0] as region_id, columns[1] as country from dfs_test.`[WORKING_PATH]/src/test/resources/store/text/data/regions.csv`");
  }

  @Test
  public void bigFile() throws Exception {
//    test("select * from dfs.`/Users/jnadeau/Documents/Yelp/star2002-full.csv` where 1=0;");
//    test(String.format("alter session set `%s` = 1; ", ExecConstants.MAX_WIDTH_PER_NODE_KEY) + "select count(*) from dfs.`/Users/jnadeau/Documents/Yelp/star2002-full.csv`;");
    test(String.format("alter session set `%s` = 1; ", ExecConstants.MAX_WIDTH_PER_NODE_KEY) + "select count(columns[0]) from dfs.`/Users/jnadeau/Documents/Yelp/star2002-full.csv`;");
//    test(String.format("alter session set `%s` = 1; ", ExecConstants.MAX_WIDTH_PER_NODE_KEY) + "use dfs.tmp; alter session set `store.format` = 'csv'; create table newStar as select columns[0], columns[1], columns[2], columns[3], columns[4], columns[5], columns[6], columns[7], columns[8], columns[9], columns[10], columns[11], columns[12], columns[13], columns[14], columns[15] from dfs.`/Users/jnadeau/Documents/Yelp/1m.csv`;");
//      test(String.format("select columns[0], columns[1], columns[2], columns[3], columns[4], columns[5], columns[6], columns[7], columns[8], columns[9], columns[10], columns[11], columns[12], columns[13], columns[14], columns[15] from dfs.`/Users/jnadeau/Documents/Yelp/star2002-50.csv` order by columns[3];"));
//  test(String.format("alter session set `%s` = 1; ", ExecConstants.MAX_WIDTH_PER_NODE_KEY) + "use dfs.tmp; alter session set `store.format` = 'csv'; create table newStar as select columns[0], columns[1], columns[2], columns[3], columns[4], columns[5], columns[6], columns[7], columns[8], columns[9], columns[10], columns[11], columns[12], columns[13], columns[14], columns[15] from dfs.`/Users/jnadeau/Documents/Yelp/star2002-full.csv`;");
//    test("select * from dfs.`/Users/jnadeau/Documents/Yelp/5m.csv` where 1 = 0;");
    // sleep for two minutes to allow grabbing profile.
    Thread.sleep(2*60*1000);
  }

  @Test
  public void testTextReader(@Injectable final DrillbitContext bitContext) throws Exception {
    final DrillConfig c = DrillConfig.createClient();

    new NonStrictExpectations(){{
      bitContext.getMetrics(); result = new MetricRegistry();
      bitContext.getAllocator(); result = new TopLevelAllocator();
      bitContext.getOperatorCreatorRegistry(); result = new OperatorCreatorRegistry(c);
      bitContext.getConfig(); result = c;
      bitContext.getCompiler(); result = CodeCompiler.getTestCompiler(c);
    }};
    FunctionImplementationRegistry registry = bitContext.getFunctionImplementationRegistry();
    FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), null, registry);

    TextFormatConfig format = new TextFormatConfig();
    FileSystem fs = FileSystem.get(new Configuration());
    Collection<DrillbitEndpoint> endpoints = Collections.emptyList();
    BlockMapBuilder b = new BlockMapBuilder(fs, endpoints);
    Path path = new Path("/Users/jnadeau/Documents/Yelp/star2002-50.csv");
    List<CompleteFileWork> works = b.generateFileWork(Collections.singletonList(fs.getFileStatus(path)), true);
    List<RecordReader> readers = Lists.newArrayList();
    for(CompleteFileWork work : works){
      FileSplit split = new FileSplit(path, work.getStart(), work.getLength(), new String[]{""});
      readers.add(new CompliantTextRecordReader(split, context, format.getDelimiter().charAt(0), Collections.singletonList(SchemaPath.getSimplePath("*"))));
    }
    ScanBatch batch = new ScanBatch(new MockSubScanPOP(null, null), context, readers.iterator());
    while(batch.next() != IterOutcome.NONE){
      VectorUtil.showVectorAccessibleContent(batch);
    }


  }

  @Test
  public void testDefaultDelimiterColumnSelection() throws Exception {
    List<QueryResultBatch> batches = testSqlWithResults("SELECT columns[0] as entire_row " +
      "from dfs_test.`[WORKING_PATH]/src/test/resources/store/text/data/letters.txt`");

    List<List<String>> expectedOutput = Arrays.asList(
      Arrays.asList("a, b,\",\"c\",\"d,, \\n e"),
      Arrays.asList("d, e,\",\"f\",\"g,, \\n h"),
      Arrays.asList("g, h,\",\"i\",\"j,, \\n k"));

    List<List<String>> actualOutput = getOutput(batches);
    System.out.println(actualOutput);
    validateOutput(expectedOutput, actualOutput);
  }

  @Test
  public void testCsvColumnSelectionCommasInsideQuotes() throws Exception {
    List<QueryResultBatch> batches = testSqlWithResults("SELECT columns[0] as col1, columns[1] as col2, columns[2] as col3," +
      "columns[3] as col4 from dfs_test.`[WORKING_PATH]/src/test/resources/store/text/data/letters.csv`");

    List<List<String>> expectedOutput = Arrays.asList(
      Arrays.asList("a, b,", "c", "d,, \\n e","f\\\"g"),
      Arrays.asList("d, e,", "f", "g,, \\n h","i\\\"j"),
      Arrays.asList("g, h,", "i", "j,, \\n k","l\\\"m"));

    List<List<String>> actualOutput = getOutput(batches);
    validateOutput(expectedOutput, actualOutput);
  }

  private List<List<String>> getOutput(List<QueryResultBatch> batches) throws SchemaChangeException {
    List<List<String>> output = new ArrayList<>();
    RecordBatchLoader loader = new RecordBatchLoader(getAllocator());
    int last = 0;
    for(QueryResultBatch batch : batches) {
      int rows = batch.getHeader().getRowCount();
      if(batch.getData() != null) {
        loader.load(batch.getHeader().getDef(), batch.getData());
        for (int i = 0; i < rows; ++i) {
          output.add(new ArrayList<String>());
          for (VectorWrapper<?> vw: loader) {
            ValueVector.Accessor accessor = vw.getValueVector().getAccessor();
            Object o = accessor.getObject(i);
            output.get(last).add(o == null ? null: o.toString());
          }
          ++last;
        }
      }
      loader.clear();
      batch.release();
    }
    return output;
  }

  private void validateOutput(List<List<String>> expected, List<List<String>> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0 ; i < expected.size(); ++i) {
      assertEquals(expected.get(i).size(), actual.get(i).size());
      for (int j = 0; j < expected.get(i).size(); ++j) {
        assertEquals(expected.get(i).get(j), actual.get(i).get(j));
      }
    }
  }

}
