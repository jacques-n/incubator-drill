package org.apache.drill.exec.planner.sql.handlers;

import java.io.IOException;

import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.planner.sql.DirectPlan;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.sql.SqlSetOption;

public class SetOptionHandler implements SqlHandler{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SetOptionHandler.class);

  QueryContext context;


  public SetOptionHandler(QueryContext context) {
    super();
    this.context = context;
  }


  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {
    SqlSetOption option = DefaultSqlHandler.unwrap(sqlNode, SqlSetOption.class);
    String scope = option.getScope();
    String name = option.getName();
    SqlNode value = option.getValue();
    if(name.equals("NO_EXCHANGES")){
      context.getSession().enableExchanges(false);
    }
    return DirectPlan.createDirectPlan(context, true, "disabled exchanges.");

  }



}
