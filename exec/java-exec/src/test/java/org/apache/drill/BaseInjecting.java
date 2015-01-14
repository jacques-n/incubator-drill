package org.apache.drill;

import org.apache.drill.exec.work.ErrorInjector;

public class BaseInjecting extends BaseTestQuery {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BaseInjecting.class);

  public static enum QueryType {
    SIMPLE("select * from cp.`employee.json`");
    ;
    public final String sql;

    QueryType(String sql){
      this.sql = sql;
    }
  }

  public void injectError(QueryType type, Class<?> injectClass, String desc){
    injectException(type, injectClass, desc, Error.class);
  }
  public void injectRuntimeException(QueryType type, Class<?> injectClass, String desc){
    injectException(type, injectClass, desc, RuntimeException.class);
  }

  public void injectException(QueryType type, Class<?> injectClass, String desc, Class<? extends Throwable> excepClass){
    ErrorInjector.clear();
    ErrorInjector.enableInjection(injectClass, desc, excepClass);
    boolean failed = false;
    try{
      test(type.sql);
    }catch(Exception e){
      failed = true;
      if(!e.getMessage().contains(ErrorInjector.INJECTION_MESSAGE_CHECK)){
        throw new InjectedErrorNotPropogated(injectClass, desc, excepClass, e);
      }
    }
    if(!failed){
      if(ErrorInjector.wasInjected()){
        throw new QueryCompletedDespiteInjectedError(injectClass, desc, excepClass);
      }else{
        throw new FailedToInject(injectClass, desc, excepClass);
      }
    }
  }

  public static class FailedToInject extends RuntimeException {
    public FailedToInject(Class<?> injectClass, String desc, Class<? extends Throwable> excepClass) {
      super(String.format("A test attempting to inject exception of type %s in class %s at descriptor [%s], never actually injected the exception.  This is likely an incorrectly written test.", excepClass.getName(), injectClass.getName(), desc));
    }

  }
  public static class QueryCompletedDespiteInjectedError extends RuntimeException {
    public QueryCompletedDespiteInjectedError(Class<?> injectClass, String desc, Class<? extends Throwable> excepClass) {
      super(String.format("When injecting exception of type %s in class %s at descriptor [%s], the query completed successfully when it should have failed.", excepClass.getName(), injectClass.getName(), desc));
    }

  }
  public static class InjectedErrorNotPropogated extends RuntimeException {

    public InjectedErrorNotPropogated(Class<?> injectClass, String desc, Class<? extends Throwable> excepClass, Throwable cause) {
      super(String.format("After injecting an exception of type %s in class %s at descriptor [%s], the returned exception did not contain the expected injected token.", excepClass.getName(), injectClass.getName(), desc), cause);
    }

  }
}
