package org.apache.drill.common.util;

import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.drill.common.config.CommonConstants;
import org.reflections.Reflections;
import org.reflections.Store;
import org.reflections.adapters.JavassistAdapter;
import org.reflections.scanners.AbstractScanner;
import org.reflections.serializers.XmlSerializer;
import org.reflections.util.ConfigurationBuilder;

import com.google.common.collect.Multimap;

import javassist.bytecode.AnnotationsAttribute;
import javassist.bytecode.ClassFile;
import javassist.bytecode.FieldInfo;

public final class FunctionResolver {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionResolver.class);

  private static final FunctionResolver.FunctionScanner FUNCTION_SCANNER = new FunctionResolver.FunctionScanner();
  // TODO: make JSON but this require GSON on the cp
  private static final XmlSerializer REFLECTIONS_SERIALIZER = new XmlSerializer();
  private static final String REFLECTION_FILE = "org/apache/drill/reflections.xml";

  static final class FunctionScanner extends AbstractScanner {
    private String FUNCTION_ANN = "org.apache.drill.exec.expr.annotations.FunctionTemplate";

    @Override
    public void scan(final Object cls) {
      try {
        ClassFile classFile = (ClassFile)cls;
        Multimap<String, String> store = getStore();
        FunctionStorage functionStorage = new FunctionStorage();

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
              fieldDescriptors.add(new FieldDescriptor(fieldName, field.getDescriptor(), getAnnotationDescriptors(fieldAnnotations)));
            }
            functionStorage.save(null, store, new FunctionDescriptor(classFile.getName(), classAnnotations, fieldDescriptors));
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
        if (memberNames != null) {
          List<AttributeDescriptor> attributes = new ArrayList<>(memberNames.size());
          for (String name : memberNames) {
            attributes.add(new AttributeDescriptor(name, annotation.getMemberValue(name).toString()));
          }
          annotationDescriptors.add(new AnnotationDescriptor(annotation.getTypeName(), attributes));
        }
      }
      return annotationDescriptors;
    }
  }


  public static abstract class Storage<T> {

    protected final String name;

    protected Storage(String name) {
      super();
      this.name = name;
    }

    protected String key(String... path) {
      StringBuilder sb = null;
      for (String s : path) {
        if (sb == null) {
          sb = new StringBuilder();
        } else {
          sb.append(".");
        }
        sb.append(s);
      }
      return sb.toString();
    }

    protected <K, V> V getUnique(Multimap<K, V> mm, K key) {
      Collection<V> values = mm.get(key);
      if (values.size() != 1) {
        throw new ArrayIndexOutOfBoundsException(key + " expected to have exactly 1 value, got " + values.size() + "(" + values + ")");
      }
      return values.iterator().next();
    }

    protected abstract String getId(T t);

    public String collection() {
      return name + "s";
    }

    public String collectionKey(String parentKey) {
      return parentKey == null ? collection() : key(parentKey, collection());
    }

    public String rootKey(String parentKey, String id) {
      String relativeKey = key(name, id);
      return parentKey == null ? relativeKey : key(parentKey, relativeKey);
    }

    public void saveAll(String parentKey, Multimap<String, String> store, List<T> c) {
      for (T t : c) {
        save(parentKey, store, t);
      }
    }

    public void save(String parentKey, Multimap<String, String> store, T t) {
      store.put(collectionKey(parentKey), getId(t));
      saveContent(rootKey(parentKey, getId(t)), store, t);
    }

    public abstract void saveContent(String rootKey, Multimap<String, String> store, T t);

    public abstract T load(String rootKey, String id, Multimap<String, String> store);

    public List<T> loadAll(String parentKey, Multimap<String, String> store) {
      Collection<String> ids = store.get(collectionKey(parentKey));
      List<T> descriptors = new ArrayList<>(ids.size());
      for (String id : ids) {
        descriptors.add(load(rootKey(parentKey, id), id, store));
      }
      return descriptors;
    }
  }

  public static final class AttributeStorage extends Storage<AttributeDescriptor> {

    protected AttributeStorage() {
      super("attribute");
    }

    @Override
    protected String getId(AttributeDescriptor t) {
      return t.name;
    }

    @Override
    public void saveContent(String rootKey, Multimap<String, String> store, AttributeDescriptor t) {
      store.put(key(rootKey, "value"), t.value);
    }

    @Override
    public AttributeDescriptor load(String rootKey, String id, Multimap<String, String> store) {
     return new AttributeDescriptor(id, getUnique(store, key(rootKey, "value")));
    }

  }

  public static final class AnnotationStorage extends Storage<AnnotationDescriptor> {

    private final AttributeStorage attributeStorage = new AttributeStorage();

    protected AnnotationStorage() {
      super("annotation");
    }

    @Override
    protected String getId(AnnotationDescriptor t) {
      return t.annotationType;
    }

    @Override
    public void saveContent(String rootKey, Multimap<String, String> store, AnnotationDescriptor t) {
      attributeStorage.saveAll(rootKey, store, t.attributes);
    }

    @Override
    public AnnotationDescriptor load(String rootKey, String id, Multimap<String, String> store) {
     return new AnnotationDescriptor(id, attributeStorage.loadAll(rootKey, store));
    }

  }

  public static final class FieldStorage extends Storage<FieldDescriptor> {

    private final AnnotationStorage annotationStorage = new AnnotationStorage();

    protected FieldStorage() {
      super("field");
    }

    @Override
    protected String getId(FieldDescriptor t) {
      return t.name;
    }

    @Override
    public void saveContent(String rootKey, Multimap<String, String> store, FieldDescriptor t) {
      store.put(key(rootKey, "descriptor"), t.descriptor);
      annotationStorage.saveAll(rootKey, store, t.annotations);
    }

    @Override
    public FieldDescriptor load(String rootKey, String id, Multimap<String, String> store) {
     return new FieldDescriptor(id, getUnique(store, key(rootKey, "descriptor")), annotationStorage.loadAll(rootKey, store));
    }

  }

  public static final class FunctionStorage extends Storage<FunctionDescriptor> {

    private final AnnotationStorage annotationStorage = new AnnotationStorage();
    private final FieldStorage fieldStorage = new FieldStorage();

    protected FunctionStorage() {
      super("function");
    }

    @Override
    protected String getId(FunctionDescriptor t) {
      return t.className;
    }

    @Override
    public void saveContent(String rootKey, Multimap<String, String> store, FunctionDescriptor t) {
      annotationStorage.saveAll(rootKey, store, t.annotations);
      fieldStorage.saveAll(rootKey, store, t.fields);
    }

    @Override
    public FunctionDescriptor load(String rootKey, String id, Multimap<String, String> store) {
      return new FunctionDescriptor(
        id,
        annotationStorage.loadAll(rootKey, store),
        fieldStorage.loadAll(rootKey, store)
      );
    }

  }

  public static final class AttributeDescriptor {
    final String name;
    final String value;
    public AttributeDescriptor(String name, String value) {
      this.name = name;
      this.value = value;
    }
    @Override
    public String toString() {
      return "AttributeDescriptor [name=" + name + ", value=" + value + "]";
    }
  }

  public static final class AnnotationDescriptor {
    final String annotationType;
    final List<AttributeDescriptor> attributes;
    public AnnotationDescriptor(String annotationType, List<AttributeDescriptor> attributes) {
      this.annotationType = annotationType;
      this.attributes = attributes;
    }
    @Override
    public String toString() {
      return "AnnotationDescriptor [annotationType=" + annotationType + ", attributes=" + attributes + "]";
    }
  }

  public static final class FieldDescriptor {
    final String name;
    final String descriptor;
    final List<AnnotationDescriptor> annotations;
    public FieldDescriptor(String name, String descriptor, List<AnnotationDescriptor> annotations) {
      this.name = name;
      this.descriptor = descriptor;
      this.annotations = annotations;
    }
    @Override
    public String toString() {
      return "FieldDescriptor [name=" + name + ", descriptor=" + descriptor + ", annotations=" + annotations + "]";
    }
  }

  public static final class FunctionDescriptor {
    final String className;
    final List<AnnotationDescriptor> annotations;
    final List<FieldDescriptor> fields;
    public FunctionDescriptor(String className, List<AnnotationDescriptor> annotations, List<FieldDescriptor> fields) {
      this.className = className;
      this.annotations = Collections.unmodifiableList(annotations);
      this.fields = Collections.unmodifiableList(fields);
    }
    @Override
    public String toString() {
      return "FunctionDescriptor [className=" + className + ", annotations=" + annotations
          + ", fields=" + fields + "]";
    }
  }


  private static Collection<URL> getMarkedPaths() {
    return PathScanner.forResource(CommonConstants.DRILL_JAR_MARKER_FILE_RESOURCE_PATHNAME, true);
  }

  private final List<FunctionDescriptor> functions;

  FunctionResolver(List<FunctionDescriptor> functions) {
    this.functions = Collections.unmodifiableList(functions);
  }

  private static Reflections scan() {
    long t0 = System.currentTimeMillis();
    try {
      ConfigurationBuilder conf = new ConfigurationBuilder()
          .setUrls(getMarkedPaths())
          .setMetadataAdapter(new JavassistAdapter()) // FunctionScanner depends on this
          .setScanners(FUNCTION_SCANNER);
      return new Reflections(conf);
    } finally {
      long t1 = System.currentTimeMillis();
      logger.debug("scanning took " + (t1 - t0) + "ms");
      System.out.println("scanning took " + (t1 - t0) + "ms");
    }
  }

  private static FunctionResolver fromStore(Store store) {
    java.util.List<FunctionDescriptor> functions = new FunctionStorage().loadAll(null, store.get(FunctionScanner.class));
    return new FunctionResolver(functions);
  }

  public static FunctionResolver fromScan() {
    return fromStore(scan().getStore());
  }

  public static FunctionResolver fromClassPath() {
    return fromStore(load().getStore());
  }

  private static void save(Reflections reflections, File reflectionsFile) {
    scan().save(reflectionsFile.getAbsolutePath(), REFLECTIONS_SERIALIZER);
  }

  private static Reflections load() {
    InputStream reflections = FunctionResolver.class.getClassLoader().getResourceAsStream(REFLECTION_FILE);
    return REFLECTIONS_SERIALIZER.read(reflections);
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      throw new IllegalArgumentException("Usage: java {cp} " + PathScanner.class.getName() + " path/to/generate");
    }
    String basePath = args[0];
    File reflectionsFile = new File(basePath, REFLECTION_FILE);
    File dir = reflectionsFile.getParentFile();
    if ((!dir.exists() && !dir.mkdirs()) || !dir.isDirectory()) {
      throw new IllegalArgumentException("could not create dir " + dir.getAbsolutePath());
    }
    save(scan(), reflectionsFile);
  }

  public List<FunctionDescriptor> getFunctions() {
    return functions;
  }


}
