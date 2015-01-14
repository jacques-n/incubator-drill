package org.apache.drill.exec.work;

import org.apache.drill.BaseInjecting;
import org.apache.drill.common.util.TestTools;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.foreman.ForemanException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestFailures extends BaseInjecting {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestFailures.class);

  @Rule public final TestRule TIMEOUT = TestTools.getTimeoutRule(5000);

  @Test
  public void foremanConstructorRuntime(){
    injectRuntimeException(QueryType.SIMPLE, Foreman.class, "constructor");
  }

  @Test
  public void foremanConstructorError(){
    injectError(QueryType.SIMPLE, Foreman.class, "constructor");
  }

  @Test
  public void foremanRun1RE(){
    injectRuntimeException(QueryType.SIMPLE, Foreman.class, "run1");
  }

  @Test
  public void foremanRun1Error(){
    injectError(QueryType.SIMPLE, Foreman.class, "run1");
  }

  @Test
  public void foremanRun2RE(){
    injectRuntimeException(QueryType.SIMPLE, Foreman.class, "run2");
  }

  @Test
  public void foremanRun2Error(){
    injectError(QueryType.SIMPLE, Foreman.class, "run2");
  }

  @Test
  public void foremanRun2Foreman(){
    injectException(QueryType.SIMPLE, Foreman.class, "run2", ForemanException.class);
  }


}
