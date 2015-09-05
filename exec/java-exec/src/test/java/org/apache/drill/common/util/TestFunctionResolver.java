package org.apache.drill.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.drill.common.util.FunctionResolver.AnnotationDescriptor;
import org.apache.drill.common.util.FunctionResolver.FieldDescriptor;
import org.apache.drill.common.util.FunctionResolver.FunctionDescriptor;
import org.apache.drill.common.util.FunctionResolver.ScanResult;
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
      fail("could not find suffix " + suffix + " in " + c);
    }
  }

  @Test
  public void test() throws Exception {
    ScanResult result = FunctionResolver.fromPrescan();
    // if the build has run properly. FunctionResolver.FUNCTION_REGISTRY_FILE was created with a prescan
    assertIsSuffixedBy(result.getPrescannedURLs(),
        // those paths have been scanned in the build
        "/exec/java-exec/target/classes/",
        "/common/target/classes/",
        "/common/target/test-classes/");
    // this paths will be scanned fo extra functions
    assertIsSuffixedBy(result.getScannedURLs(), "/exec/java-exec/target/test-classes/");
    List<FunctionDescriptor> functions = result.getFunctions();
    // TODO: use Andrew's randomized test framework to verify a subset of the functions
    for (FunctionDescriptor function : functions) {
      Class<?> c = Class.forName(function.getClassName(), false, this.getClass().getClassLoader());

      Field[] fields = c.getDeclaredFields();
      assertEquals("fields count for " + function, fields.length, function.getFields().size());
      for (int i = 0; i < fields.length; i++) {
        FieldDescriptor fieldDescriptor = function.getFields().get(i);
        Field field = fields[i];
        Assert.assertEquals(
            "Class fields:\n" + Arrays.toString(fields) + "\n != \nDescriptor fields:\n" + function.getFields(),
            field.getName(), fieldDescriptor.getName());
        verifyAnnotations(field.getDeclaredAnnotations(), fieldDescriptor.getAnnotations());
        // maybe verify descriptor
      }

      Annotation[] annotations = c.getDeclaredAnnotations();
      List<AnnotationDescriptor> scannedAnnotations = function.getAnnotations();
      verifyAnnotations(annotations, scannedAnnotations);
    }
    // TODO: add a function in the test source dir to verify it is loaded properly
  }

  private void verifyAnnotations(Annotation[] annotations, List<AnnotationDescriptor> scannedAnnotations) {
    assertEquals(Arrays.toString(annotations) + " expected but got " + scannedAnnotations, annotations.length, scannedAnnotations.size());
    for (int i = 0; i < annotations.length; i++) {
      Annotation annotation = annotations[i];
      AnnotationDescriptor scannedAnnotation = scannedAnnotations.get(i);
      assertEquals(annotation.annotationType().getName(), scannedAnnotation.getAnnotationType());
      // maybe verify the value
    }
  }

}
