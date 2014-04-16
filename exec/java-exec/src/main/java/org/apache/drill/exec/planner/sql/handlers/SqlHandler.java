package org.apache.drill.exec.planner.sql.handlers;

import java.io.IOException;

import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.exec.physical.PhysicalPlan;
import org.eigenbase.sql.SqlNode;

public interface SqlHandler {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SqlHandler.class);

  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException;
}
