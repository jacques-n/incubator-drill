package org.apache.drill.exec.planner.sql;


public class CmdException extends Exception{

  public CmdException() {
    super();

  }

  public CmdException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);

  }

  public CmdException(String message, Throwable cause) {
    super(message, cause);

  }

  public CmdException(String message) {
    super(message);

  }

  public CmdException(Throwable cause) {
    super(cause);

  }

}
