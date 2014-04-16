package org.apache.drill.exec.planner.sql;

import java.util.Collections;
import java.util.Iterator;

import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.common.logical.PlanProperties.PlanPropertiesBuilder;
import org.apache.drill.common.logical.PlanProperties.PlanType;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.config.Screen;
import org.apache.drill.exec.planner.sql.handlers.DefaultSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.SimpleCommandResult;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.direct.DirectGroupScan;
import org.apache.drill.exec.store.pojo.PojoRecordReader;

public class DirectPlan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectPlan.class);


  public static PhysicalPlan createDirectPlan(QueryContext context, boolean result, String message){
    return createDirectPlan(context, new SimpleCommandResult(result, message));
  }

  @SuppressWarnings("unchecked")
  public static <T> PhysicalPlan createDirectPlan(QueryContext context, T obj){
    Iterator<T> iter = (Iterator<T>) Collections.singleton(obj).iterator();
    return createDirectPlan(context.getCurrentEndpoint(), iter, (Class<T>) obj.getClass());

  }
  public static <T> PhysicalPlan createDirectPlan(DrillbitEndpoint endpoint, Iterator<T> iterator, Class<T> clazz){
    PojoRecordReader<T> reader = new PojoRecordReader<T>(clazz, iterator);
    DirectGroupScan scan = new DirectGroupScan(reader);
    Screen screen = new Screen(scan, endpoint);

    PlanPropertiesBuilder propsBuilder = PlanProperties.builder();
    propsBuilder.type(PlanType.APACHE_DRILL_PHYSICAL);
    propsBuilder.version(1);
    propsBuilder.resultMode(ResultMode.EXEC);
    propsBuilder.generator(DirectPlan.class.getSimpleName(), "");
    return new PhysicalPlan(propsBuilder.build(), DefaultSqlHandler.getPops(screen));

  }
}
