package org.apache.drill.exec.planner.sql.handlers;

public class SimpleCommandResult {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SimpleCommandResult.class);

  public boolean ok;
  public String summary;

  public SimpleCommandResult(boolean ok, String summary) {
    super();
    this.ok = ok;
    this.summary = summary;
  }

}
