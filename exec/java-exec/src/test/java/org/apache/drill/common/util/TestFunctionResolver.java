package org.apache.drill.common.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestFunctionResolver {

  @Test
  public void test() {
    System.out.println(FunctionResolver.fromClassPath().getFunctions());
  }

}
