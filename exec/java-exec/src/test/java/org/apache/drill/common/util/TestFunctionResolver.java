package org.apache.drill.common.util;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.util.FunctionResolver.FieldDescriptor;
import org.apache.drill.common.util.FunctionResolver.FunctionDescriptor;
import org.apache.drill.common.util.FunctionResolver.ScanResult;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.junit.Assert;
import org.junit.Test;

public class TestFunctionResolver {

  private void assertIsSuffixedBy(Collection<String> c, String... suffixes) {
    assertEquals("expected " + Arrays.toString(suffixes) + " to have same size as" + c, suffixes.length, c.size());
    outer: for (String suffix : suffixes) {
      for (String item : c) {
        if (item.endsWith(suffix)) {
          continue outer;
        }
      }
      Assert.fail("could not find suffix " + suffix + " in " + c);
    }
  }

  @Test
  public void test() throws Exception {
    ScanResult result = FunctionResolver.fromPrescan();
    assertIsSuffixedBy(result.getPrescannedURLs(),
        "/exec/java-exec/target/classes/",
        "/common/target/classes/",
        "/common/target/test-classes/");
    assertIsSuffixedBy(result.getScannedURLs(), "/exec/java-exec/target/test-classes/");
    List<FunctionDescriptor> functions = result.getFunctions();
    for (FunctionDescriptor function : functions) {
      Class<?> c = Class.forName(function.className, false, this.getClass().getClassLoader());
      FunctionTemplate annotation = c.getAnnotation(FunctionTemplate.class);
      Field[] fields = c.getDeclaredFields();
      Assert.assertEquals("fields count for " + function, fields.length, function.fields.size());
      for (int i = 0; i < fields.length; i++) {
        FieldDescriptor fieldDescriptor = function.fields.get(i);
        Field field = fields[i];
        Assert.assertEquals("Class fields:\n" + Arrays.toString(fields) + "\n != \nDescriptor fields:\n" + function.fields, field.getName(), fieldDescriptor.name);
      }
//      System.out.println(annotation);
    }
//    System.out.println(functions);
  }

}
