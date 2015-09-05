/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.common.util;

import static com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT;
import static org.reflections.util.FilterBuilder.prefix;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.Descriptor;
import javassist.bytecode.FieldInfo;
import javassist.bytecode.annotation.AnnotationMemberValue;
import javassist.bytecode.annotation.ArrayMemberValue;
import javassist.bytecode.annotation.BooleanMemberValue;
import javassist.bytecode.annotation.ByteMemberValue;
import javassist.bytecode.annotation.CharMemberValue;
import javassist.bytecode.annotation.ClassMemberValue;
import javassist.bytecode.annotation.DoubleMemberValue;
import javassist.bytecode.annotation.EnumMemberValue;
import javassist.bytecode.annotation.FloatMemberValue;
import javassist.bytecode.annotation.IntegerMemberValue;
import javassist.bytecode.annotation.LongMemberValue;
import javassist.bytecode.annotation.MemberValue;
import javassist.bytecode.annotation.MemberValueVisitor;
import javassist.bytecode.annotation.ShortMemberValue;
import javassist.bytecode.annotation.StringMemberValue;

import org.apache.drill.common.config.CommonConstants;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.reflections.Reflections;
import org.reflections.adapters.JavassistAdapter;
import org.reflections.scanners.AbstractScanner;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;

public final class FunctionResolver {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionResolver.class);
  private static final String FUNCTION_REGISTRY_FILE = "META-INF/function-registry/registry.json";

  private static final ObjectMapper mapper = new ObjectMapper().enable(INDENT_OUTPUT);
  private static final ObjectReader reader = mapper.reader(ScanResult.class);
  private static final ObjectWriter writer = mapper.writerWithType(ScanResult.class);

  private static class ListingMemberValueVisitor implements MemberValueVisitor {
    private final List<String> values;

    private ListingMemberValueVisitor(List<String> values) {
      this.values = values;
    }

    @Override
    public void visitStringMemberValue(StringMemberValue node) {
      values.add(node.getValue());
    }

    @Override
    public void visitShortMemberValue(ShortMemberValue node) {
      values.add(String.valueOf(node.getValue()));
    }

    @Override
    public void visitLongMemberValue(LongMemberValue node) {
      values.add(String.valueOf(node.getValue()));
    }

    @Override
    public void visitIntegerMemberValue(IntegerMemberValue node) {
      values.add(String.valueOf(node.getValue()));
    }

    @Override
    public void visitFloatMemberValue(FloatMemberValue node) {
      values.add(String.valueOf(node.getValue()));
    }

    @Override
    public void visitEnumMemberValue(EnumMemberValue node) {
      values.add(node.getValue());
    }

    @Override
    public void visitDoubleMemberValue(DoubleMemberValue node) {
      values.add(String.valueOf(node.getValue()));
    }

    @Override
    public void visitClassMemberValue(ClassMemberValue node) {
      values.add(node.getValue());
    }

    @Override
    public void visitCharMemberValue(CharMemberValue node) {
      values.add(String.valueOf(node.getValue()));
    }

    @Override
    public void visitByteMemberValue(ByteMemberValue node) {
      values.add(String.valueOf(node.getValue()));
    }

    @Override
    public void visitBooleanMemberValue(BooleanMemberValue node) {
      values.add(String.valueOf(node.getValue()));
    }

    @Override
    public void visitArrayMemberValue(ArrayMemberValue node) {
      MemberValue[] nestedValues = node.getValue();
      for (MemberValue v : nestedValues) {
        v.accept(new ListingMemberValueVisitor(values) {
          @Override
          public void visitArrayMemberValue(ArrayMemberValue node) {
            values.add(Arrays.toString(node.getValue()));
          }
        });
      }
    }

    @Override
    public void visitAnnotationMemberValue(AnnotationMemberValue node) {
      values.add(String.valueOf(node.getValue()));
    }
  }

  private static final class FunctionScanner extends AbstractScanner {

    private String FUNCTION_ANN = "org.apache.drill.exec.expr.annotations.FunctionTemplate";
    public List<FunctionDescriptor> functions = new ArrayList<>();

    @Override
    public void scan(final Object cls) {
      try {
        final ClassFile classFile = (ClassFile) cls;

        AnnotationsAttribute annotations = ((AnnotationsAttribute)classFile.getAttribute(AnnotationsAttribute.visibleTag));
        if (annotations != null) {
          boolean isFunctionTemplate = false;
          for (javassist.bytecode.annotation.Annotation a : annotations.getAnnotations()) {
            if (a.getTypeName().equals(FUNCTION_ANN)) {
              isFunctionTemplate = true;
            }
          }
          if (isFunctionTemplate) {
            List<AnnotationDescriptor> classAnnotations = getAnnotationDescriptors(annotations);
            @SuppressWarnings("unchecked")
            List<FieldInfo> classFields = classFile.getFields();
            List<FieldDescriptor> fieldDescriptors = new ArrayList<>(classFields.size());
            for (FieldInfo field : classFields) {
              String fieldName = field.getName();
              AnnotationsAttribute fieldAnnotations = ((AnnotationsAttribute)field.getAttribute(AnnotationsAttribute.visibleTag));
              fieldDescriptors.add(new FieldDescriptor(fieldName, Descriptor.toClassName(field.getDescriptor()),
                  getAnnotationDescriptors(fieldAnnotations)));
            }
            functions.add(new FunctionDescriptor(classFile.getName(), classAnnotations,
                fieldDescriptors));
          }
        }
      } catch (RuntimeException e) {
        // Sigh: Collections swallows exceptions thrown here...
        e.printStackTrace();
        throw e;
      }
    }

    private List<AnnotationDescriptor> getAnnotationDescriptors(AnnotationsAttribute annotationsAttr) {
      List<AnnotationDescriptor> annotationDescriptors = new ArrayList<>(annotationsAttr.numAnnotations());
      for (javassist.bytecode.annotation.Annotation annotation : annotationsAttr.getAnnotations()) {
        // Sigh: javassist uses raw collections (is this 2002?)
        @SuppressWarnings("unchecked")
        Set<String> memberNames = annotation.getMemberNames();
        List<AttributeDescriptor> attributes = new ArrayList<>();
        if (memberNames != null) {
          for (String name : memberNames) {
            MemberValue memberValue = annotation.getMemberValue(name);
            final List<String> values = new ArrayList<>();
            memberValue.accept(new ListingMemberValueVisitor(values));
            attributes.add(new AttributeDescriptor(name, values));
          }
        }
        annotationDescriptors.add(new AnnotationDescriptor(annotation.getTypeName(), attributes));
      }
      return annotationDescriptors;
    }
  }

  public static final class AttributeDescriptor {
    private final String name;
    private final List<String> values;

    @JsonCreator
    public AttributeDescriptor(
        @JsonProperty("name") String name,
        @JsonProperty("values") List<String> values) {
      this.name = name;
      this.values = values;
    }
    public String getName() {
      return name;
    }

    @JsonIgnore
    public List<String> getValues() {
      return values;
    }

    @Override
    public String toString() {
      return "Attribute[" + name + "=" + values + "]";
    }
  }

  public static final class AnnotationDescriptor {
    private final String annotationType;
    private final List<AttributeDescriptor> attributes;
    private final ImmutableMap<String, AttributeDescriptor> attributeMap;

    @JsonCreator
    public AnnotationDescriptor(
        @JsonProperty("annotationType") String annotationType,
        @JsonProperty("attributes") List<AttributeDescriptor> attributes) {
      this.annotationType = annotationType;
      this.attributes = attributes;
      ImmutableMap.Builder<String, AttributeDescriptor> mapBuilder = ImmutableMap.builder();
      for (AttributeDescriptor att : attributes) {
        mapBuilder.put(att.getName(), att);
      }
      this.attributeMap = mapBuilder.build();
    }

    public String getAnnotationType() {
      return annotationType;
    }

    public List<AttributeDescriptor> getAttributes() {
      return attributes;
    }

    @SuppressWarnings("unchecked")
    public List<String> getValues(String attributeName) {
      AttributeDescriptor desc = attributeMap.get(attributeName);
      if (desc == null) {
        return Collections.EMPTY_LIST;
      } else {
        return desc.getValues();
      }
    }

    public String getSingleValue(String attributeName, String defaultValue) {
      List<String> values = getValues(attributeName);
      if (values.size() > 1) {
        throw new IllegalStateException(String.format(
            "Expected a single attribute for the attribute named %s. However, there were %d attributes", attributeName,
            values.size()));
      }
      return values.isEmpty() ? defaultValue : values.get(0);
    }

    public String getSingleValue(String attributeName) {
      String val = getSingleValue(attributeName, null);
      if (val == null) {
        throw new IllegalStateException(String.format(
            "Expected a single attribute for the attribute named %s. However, there were zero attributes",
            attributeName));
      }
      return val;
    }

    @Override
    public String toString() {
      return "Annotation[type=" + annotationType + ", attributes=" + attributes + "]";
    }

  }

  public static final class FieldDescriptor {

    private final String name;
    private final String descriptor;
    private final List<AnnotationDescriptor> annotations;
    private final ImmutableMap<String, AnnotationDescriptor> annotationMap;

    @JsonCreator
    public FieldDescriptor(
        @JsonProperty("name") String name,
        @JsonProperty("descriptor") String descriptor,
        @JsonProperty("annotations") List<AnnotationDescriptor> annotations) {
      this.name = name;
      this.descriptor = descriptor;
      this.annotations = annotations;
      ImmutableMap.Builder<String, AnnotationDescriptor> annMapBuilder = ImmutableMap.builder();
      for (AnnotationDescriptor ann : annotations) {
        annMapBuilder.put(ann.getAnnotationType(), ann);
      }
      this.annotationMap = annMapBuilder.build();
    }

    public String getName() {
      return name;
    }

    public String getDescriptor() {
      return descriptor;
    }

    public AnnotationDescriptor getAnnotation(Class<?> clazz) {
      return annotationMap.get(clazz.getName());
    }

    public List<AnnotationDescriptor> getAnnotations() {
      return annotations;
    }

    @JsonIgnore
    public Class<?> getFieldClass() {
      if (!descriptor.contains(".")) {
        switch (descriptor) {
        case "int":
          return int.class;
        case "int[]":
          return int[].class;
        case "long":
          return long.class;
        case "long[]":
          return long[].class;
        case "double":
          return double.class;
        case "double[]":
          return double[].class;
        case "float":
          return float.class;
        case "float[]":
          return float[].class;
        case "char":
          return char.class;
        case "char[]":
          return char[].class;
        case "byte":
          return byte.class;
        case "byte[]":
          return byte[].class;
        case "short":
          return short.class;
        case "short[]":
          return short[].class;
        case "boolean":
          return boolean.class;
        case "boolean[]":
          return boolean[].class;
        case "Object[]":
          return Object[].class;
        }
      }
      try {
        return Class.forName(descriptor);
      } catch (ClassNotFoundException ex) {
        throw new DrillRuntimeException("Unexpected class load failure while attempting to load Function Registry.", ex);
      }
    }

    @Override
    public String toString() {
      return "Field[name=" + name + ", descriptor=" + descriptor + ", annotations=" + annotations + "]";
    }
  }

  public static final class FunctionDescriptor {
    private final String className;
    private final List<AnnotationDescriptor> annotations;
    private final List<FieldDescriptor> fields;
    private final ImmutableMap<String, AnnotationDescriptor> annotationMap;

    @JsonCreator
    public FunctionDescriptor(
        @JsonProperty("className") String className,
        @JsonProperty("annotations") List<AnnotationDescriptor> annotations,
        @JsonProperty("fields") List<FieldDescriptor> fields) {
      this.className = className;
      this.annotations = Collections.unmodifiableList(annotations);
      this.fields = Collections.unmodifiableList(fields);
      ImmutableMap.Builder<String, AnnotationDescriptor> annMapBuilder = ImmutableMap.builder();
      for (AnnotationDescriptor ann : annotations) {
        annMapBuilder.put(ann.getAnnotationType(), ann);
      }
      this.annotationMap = annMapBuilder.build();
    }

    public String getClassName() {
      return className;
    }

    public List<AnnotationDescriptor> getAnnotations() {
      return annotations;
    }

    public AnnotationDescriptor getAnnotation(Class<?> clazz) {
      return annotationMap.get(clazz.getName());
    }

    public List<FieldDescriptor> getFields() {
      return fields;
    }

    @Override
    public String toString() {
      return "Function [className=" + className + ", annotations=" + annotations
          + ", fields=" + fields + "]";
    }
  }

  public static final class ScanResult {
    private final List<String> prescannedURLs;
    private final List<String> scannedURLs;
    private final List<FunctionDescriptor> functions;
    @JsonCreator
    public ScanResult(
        @JsonProperty("prescannedURLs") List<String> prescannedURLs,
        @JsonProperty("scannedURLs") List<String> scannedURLs,
        @JsonProperty("functions") List<FunctionDescriptor> functions) {
      super();
      this.prescannedURLs = prescannedURLs;
      this.scannedURLs = scannedURLs;
      this.functions = functions;
    }
    public List<String> getPrescannedURLs() {
      return prescannedURLs;
    }
    public List<String> getScannedURLs() {
      return scannedURLs;
    }
    public List<FunctionDescriptor> getFunctions() {
      return functions;
    }
    @Override
    public String toString() {
      return "ScanResult [prescannedURLs=" + prescannedURLs + ", scannedURLs=" + scannedURLs + ", functions=" + functions
          + "]";
    }
    public ScanResult merge(ScanResult other) {
      final List<String> newPrescannedURLs = new ArrayList<>(prescannedURLs);
      newPrescannedURLs.addAll(other.prescannedURLs);
      final List<String> newScannedURLs = new ArrayList<>(scannedURLs);
      newScannedURLs.addAll(other.scannedURLs);
      final List<FunctionDescriptor> newFunctions = new ArrayList<>(functions);
      newFunctions.addAll(other.functions);
      return new ScanResult(newPrescannedURLs, newScannedURLs, newFunctions);
    }
  }

  static Collection<URL> getMarkedPaths() {
    return PathScanner.forResource(CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME, true);
  }

  static Collection<URL> getPrescannedPaths() {
    return PathScanner.forResource(FUNCTION_REGISTRY_FILE, true);
  }

  static Collection<URL> getNonPrescannedMarkedPaths() {
    Collection<URL> markedPaths = getMarkedPaths();
    markedPaths.removeAll(getPrescannedPaths());
    return markedPaths;
  }

  static ScanResult fromFullScan(List<String> prefixes) {
    return scan(getMarkedPaths(), prefixes);
  }

  private static List<String> mapToString(Collection<? extends Object> l) {
    List<String> result = new ArrayList<>(l.size());
    for (Object o : l) {
      result.add(o.toString());
    }
    return result;
  }

  static ScanResult scan(Collection<URL> pathsToScan, List<String> prefixes) {
    Stopwatch watch = new Stopwatch().start();
    try {
      FunctionResolver.FunctionScanner functionScanner = new FunctionResolver.FunctionScanner();

      FilterBuilder filter = new FilterBuilder();
      for (String prefix : prefixes) {
        filter.include(prefix(prefix));
      }

      ConfigurationBuilder conf = new ConfigurationBuilder()
          .setUrls(pathsToScan)
          .setMetadataAdapter(new JavassistAdapter()) // FunctionScanner depends on this
          .filterInputsBy(filter)
          .setScanners(functionScanner);

      new Reflections(conf); // scans stuff, but don't use the funky storage layer
      return new ScanResult(Collections.<String>emptyList(), mapToString(pathsToScan), functionScanner.functions);
    } finally {
      logger.debug("scanning " + pathsToScan + " took " + watch.elapsed(TimeUnit.MILLISECONDS) + "ms");
    }
  }

  public static ScanResult fromPrescan(List<String> prefixes) {
    ScanResult prescanned = load();
    final Set<String> prescannedURLs = new HashSet<>();
    if (prescanned != null) {
      prescannedURLs.addAll(prescanned.scannedURLs);
      prescannedURLs.addAll(prescanned.prescannedURLs);
      prescanned = new ScanResult(
          new ArrayList<>(prescannedURLs),
          Collections.<String>emptyList(),
          prescanned.functions);
    }

    final Collection<URL> toScan = getNonPrescannedMarkedPaths();
    for (Iterator<URL> iterator = toScan.iterator(); iterator.hasNext();) {
      URL url = iterator.next();
      if (prescannedURLs.contains(url.toString())) {
        iterator.remove();
      }
    }

    ScanResult others = scan(toScan, prefixes);
    ScanResult merged = prescanned == null ? others : prescanned.merge(others);
    return merged;
  }

  private static void save(ScanResult scanResult, File file) {
    try {
      writer.writeValue(file, scanResult);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static ScanResult load() {
    Set<URL> preScanned = PathScanner.forResource(FUNCTION_REGISTRY_FILE, false);
    ScanResult result = null;
    for (URL u : preScanned) {
      try (InputStream reflections = u.openStream()) {
        ScanResult ref = reader.readValue(reflections);
        if (result == null) {
          result = ref;
        } else {
          result = result.merge(ref);
        }
      } catch (IOException e) {
        throw new RuntimeException("can't read function registry at " + u, e);
      }
    }
    return result;
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      throw new IllegalArgumentException("Usage: java {cp} " + PathScanner.class.getName() + " path/to/generate");
    }
    String basePath = args[0];
    File reflectionsFile = new File(basePath, FUNCTION_REGISTRY_FILE);
    File dir = reflectionsFile.getParentFile();
    if ((!dir.exists() && !dir.mkdirs()) || !dir.isDirectory()) {
      throw new IllegalArgumentException("could not create dir " + dir.getAbsolutePath());
    }

    save(fromFullScan(Collections.singletonList("org")), reflectionsFile);
  }

}
