package org.apache.drill.exec.store.text;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import mockit.Injectable;
import mockit.NonStrictExpectations;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecTest;
import org.apache.drill.exec.compile.CodeCompiler;
import org.apache.drill.exec.expr.fn.FunctionImplementationRegistry;
import org.apache.drill.exec.memory.TopLevelAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.OperatorCreatorRegistry;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.proto.BitControl.PlanFragment;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.record.RecordBatch.IterOutcome;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.RecordReader;
import org.apache.drill.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import org.apache.drill.exec.store.easy.text.compliant.CompliantTextRecordReader;
import org.apache.drill.exec.store.mock.MockSubScanPOP;
import org.apache.drill.exec.store.schedule.BlockMapBuilder;
import org.apache.drill.exec.store.schedule.CompleteFileWork;
import org.apache.drill.exec.util.VectorUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.Test;

import com.beust.jcommander.internal.Lists;
import com.codahale.metrics.MetricRegistry;

public class TestCompliantTextReader extends ExecTest {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestCompliantTextReader.class);

  @Test
  public void testTextReader(@Injectable final DrillbitContext bitContext) throws Throwable {
    final DrillConfig c = DrillConfig.createClient();
    System.out.println("a");
    final MetricRegistry r = new MetricRegistry();
    try(final TopLevelAllocator alloc = new TopLevelAllocator(c)){;
      System.out.println("a2");
      final CodeCompiler compiler = CodeCompiler.getTestCompiler(c);
      System.out.println("a3");
      new NonStrictExpectations(){{
        bitContext.getMetrics(); result = r;
        bitContext.getAllocator(); result = alloc;
        bitContext.getOperatorCreatorRegistry(); result = OperatorCreatorRegistry.createEmptyRegistry(c);
        bitContext.getConfig(); result = c;
        bitContext.getCompiler(); result = compiler;
      }};
      FunctionImplementationRegistry registry = bitContext.getFunctionImplementationRegistry();
      FragmentContext context = new FragmentContext(bitContext, PlanFragment.getDefaultInstance(), null, registry);
      System.out.println("b");
      TextFormatConfig format = new TextFormatConfig();
      FileSystem fs = FileSystem.get(new Configuration());
      Collection<DrillbitEndpoint> endpoints = Collections.emptyList();
      BlockMapBuilder b = new BlockMapBuilder(fs, endpoints);
      System.out.println("c");
      Path path = new Path("/Users/jnadeau/Documents/Yelp/star2002-50.csv");
      List<CompleteFileWork> works = b.generateFileWork(Collections.singletonList(fs.getFileStatus(path)), true);
      List<RecordReader> readers = Lists.newArrayList();
      for(CompleteFileWork work : works){
        FileSplit split = new FileSplit(path, work.getStart(), work.getLength(), new String[]{""});
        readers.add(new CompliantTextRecordReader(split, context, format.getDelimiter().charAt(0), Collections.singletonList(SchemaPath.getSimplePath("*"))));
      }
      ScanBatch batch = new ScanBatch(new MockSubScanPOP(null, null), context, readers.iterator());
      IterOutcome outcome;
      while( (outcome = batch.next())    != IterOutcome.NONE && outcome != IterOutcome.STOP){
        System.out.println(outcome.name());
        VectorUtil.showVectorAccessibleContent(batch, 200);
      }

      if(context.isFailed()){
        throw context.getFailureCause();
      }
      batch.cleanup();
      context.close();
    }

  }
}
