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
package org.apache.drill.exec.expr.fn;

import java.lang.reflect.Field;
import java.util.List;

import javax.inject.Inject;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.util.FunctionResolver.AnnotationDescriptor;
import org.apache.drill.common.util.FunctionResolver.FieldDescriptor;
import org.apache.drill.common.util.FunctionResolver.FunctionDescriptor;
import org.apache.drill.exec.expr.DrillFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionCostCategory;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.annotations.Workspace;
import org.apache.drill.exec.expr.fn.DrillFuncHolder.ValueReference;
import org.apache.drill.exec.expr.fn.DrillFuncHolder.WorkspaceReference;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.vector.complex.reader.FieldReader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter.ComplexWriter;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Converts FunctionCalls to Java Expressions.
 */
public class FunctionConverter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FunctionConverter.class);

  public <T extends DrillFunc> DrillFuncHolder getHolder(FunctionDescriptor function) {

    AnnotationDescriptor template = function.getAnnotation(FunctionTemplate.class);
    if (template == null) {
      return failure("Class does not declare FunctionTemplate annotation.", function);
    }

    final List<String> names;
    {
      final List<String> singleName = template.getValues("name");
      final List<String> mutliName = template.getValues("names");
      if ((singleName.isEmpty() && mutliName.isEmpty())) {
        return failure("Must set either 'name' or 'names' annotation. Neither was set.", function);
      }

      if (!singleName.isEmpty() && !mutliName.isEmpty()) {
        return failure("Must use only one annotations 'name' or 'names', not both.", function);
      }
      names = singleName.isEmpty() ? mutliName : singleName;
    }

    final String primaryName = names.get(0);
    final FunctionScope scope = FunctionScope.valueOf(template.getSingleValue("scope"));
    final NullHandling nullHandling = NullHandling.valueOf(template.getSingleValue("nulls",
        NullHandling.INTERNAL.name()));
    final boolean isDeterministic = !"true".equalsIgnoreCase(template.getSingleValue("isRandom", "false"));
    final boolean isBinaryCommutative = "true"
        .equalsIgnoreCase(template.getSingleValue("isBinaryCommutative", "false"));
    final FunctionCostCategory costCategory = FunctionCostCategory.valueOf(template.getSingleValue("costCategory",
        FunctionCostCategory.SIMPLE.name()));

    // start by getting field information.
    List<ValueReference> params = Lists.newArrayList();
    List<WorkspaceReference> workspaceFields = Lists.newArrayList();

    ValueReference outputField = null;


    for (FieldDescriptor field : function.getFields()) {

      AnnotationDescriptor param = field.getAnnotation(Param.class);
      AnnotationDescriptor output = field.getAnnotation(Output.class);
      AnnotationDescriptor workspace = field.getAnnotation(Workspace.class);
      AnnotationDescriptor inject = field.getAnnotation(Inject.class);

      int i =0;
      if (param != null) {
        i++;
      }
      if (output != null) {
        i++;
      }
      if (workspace != null) {
        i++;
      }
      if (inject != null) {
        i++;
      }
      if (i == 0) {
        return failure("The field must be either a @Param, @Output, @Inject or @Workspace field.", function, field);
      } else if(i > 1) {
        return failure(
            "The field must be only one of @Param, @Output, @Inject or @Workspace.  It currently has more than one of these annotations.",
            function, field);
      }

      if (param != null || output != null) {

        // Special processing for @Param FieldReader
        if (param != null && FieldReader.class.isAssignableFrom(field.getFieldClass())) {
          params.add(ValueReference.createFieldReaderRef(field.getName()));
          continue;
        }

        // Special processing for @Output ComplexWriter
        if (output != null && ComplexWriter.class.isAssignableFrom(field.getFieldClass())) {
          if (outputField != null) {
            return failure(
                "You've declared more than one @Output field.  You must declare one and only @Output field per Function class.",
                function, field);
          }else{
            outputField = ValueReference.createComplexWriterRef(field.getName());
          }
          continue;
        }

        // check that param and output are value holders.
        if (!ValueHolder.class.isAssignableFrom(field.getFieldClass())) {
          return failure(
              String.format("The field doesn't holds value of type %s which does not implement the "
                  + "ValueHolder interface.  All fields of type @Param or @Output must extend this interface..",
                  field.getFieldClass()), function, field);
        }

        // get the type field from the value holder.
        MajorType type = null;
        try {
          type = getStaticFieldValue("TYPE", field.getFieldClass(), MajorType.class);
        } catch (Exception e) {
          return failure(
              "Failure while trying to access the ValueHolder's TYPE static variable.  All ValueHolders must contain a static TYPE variable that defines their MajorType.",
              e, function, field);
        }


        ValueReference p = new ValueReference(type, field.getName());
        if (param != null) {
          List<String> constant = param.getValues("constant");
          if (!constant.isEmpty() && "true".equalsIgnoreCase(constant.get(0))) {
            p.setConstant(true);
          }
          params.add(p);
        } else {
          if (outputField != null) {
            return failure("You've declared more than one @Output field.  You must declare one and only @Output field per Function class.", function, field);
          } else {
            outputField = p;
          }
        }

      } else {
        // workspace work.
        boolean isInject = inject != null;
        if (isInject && UdfUtilities.INJECTABLE_GETTER_METHODS.get(field.getFieldClass()) == null) {
          return failure(String.format("A %s cannot be injected into a %s, available injectable classes are: %s.",
                  field.getFieldClass(), DrillFunc.class.getSimpleName(),
                  Joiner.on(",").join(UdfUtilities.INJECTABLE_GETTER_METHODS.keySet())), function, field);
        }
        WorkspaceReference wsReference = new WorkspaceReference(field.getFieldClass(), field.getName(), isInject);


        if (!isInject && scope == FunctionScope.POINT_AGGREGATE
            && !ValueHolder.class.isAssignableFrom(field.getFieldClass())) {
          return failure(String.format(
              "Aggregate function '%s' workspace variable '%s' is of type '%s'. Please change it to Holder type.",
              primaryName, field.getName(), field.getFieldClass()), function, field);
        }

        //If the workspace var is of Holder type, get its MajorType and assign to WorkspaceReference.
        if (ValueHolder.class.isAssignableFrom(field.getFieldClass())) {
          MajorType majorType = null;
          try {
            majorType = getStaticFieldValue("TYPE", field.getFieldClass(), MajorType.class);
          } catch (Exception e) {
            return failure(
                "Failure while trying to access the ValueHolder's TYPE static variable.  All ValueHolders must contain a static TYPE variable that defines their MajorType.",
                e, function, field);
          }
          wsReference.setMajorType(majorType);
        }
        workspaceFields.add(wsReference);
      }
    }

   // if (!workspaceFields.isEmpty()) return failure("This function declares one or more workspace fields.  However, those have not yet been implemented.", clazz);
    if (outputField == null) {
      return failure("This function declares zero output fields.  A function must declare one output field.", function);
    }



    try{
      // return holder
      ValueReference[] ps = params.toArray(new ValueReference[params.size()]);
      WorkspaceReference[] works = workspaceFields.toArray(new WorkspaceReference[workspaceFields.size()]);
      final FunctionInitializer initializer = new FunctionInitializer(function.getClassName());

      String[] registeredNames = names.toArray(new String[names.size()]);
      switch (scope) {
      case POINT_AGGREGATE:
        return new DrillAggFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic, registeredNames,
            ps, outputField, works, initializer, costCategory);
      case DECIMAL_AGGREGATE:
        return new DrillDecimalAggFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic,
            registeredNames,
            ps, outputField, works, initializer);
      case DECIMAL_SUM_AGGREGATE:
        return new DrillDecimalSumAggFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic,
            registeredNames, ps, outputField, works, initializer);
      case SIMPLE:
        if (outputField.isComplexWriter) {
          return new DrillComplexWriterFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic,
              registeredNames, ps, outputField, works, initializer);
        } else {
          return new DrillSimpleFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic, registeredNames,
              ps, outputField, works, initializer);
        }
      case SC_BOOLEAN_OPERATOR:
        return new DrillBooleanOPHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic, registeredNames,
            ps, outputField, works, initializer, FunctionTemplate.FunctionCostCategory.getDefault());

      case DECIMAL_MAX_SCALE:
          return new DrillDecimalMaxScaleFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic, registeredNames,
            ps, outputField, works, initializer, FunctionCostCategory.getDefault());
      case DECIMAL_MUL_SCALE:
        return new DrillDecimalSumScaleFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic,
            registeredNames,
            ps, outputField, works, initializer, FunctionCostCategory.getDefault());
      case DECIMAL_ADD_SCALE:
        return new DrillDecimalAddFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic,
            registeredNames,
            ps, outputField, works, initializer, FunctionCostCategory.getDefault());
      case DECIMAL_CAST:
        return new DrillDecimalCastFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic,
            registeredNames,
            ps, outputField, works, initializer, FunctionCostCategory.getDefault());
      case DECIMAL_DIV_SCALE:
        return new DrillDecimalDivScaleFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic,
            registeredNames,
            ps, outputField, works, initializer, FunctionCostCategory.getDefault());
      case DECIMAL_MOD_SCALE:
        return new DrillDecimalModScaleFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic,
            registeredNames,
            ps, outputField, works, initializer, FunctionCostCategory.getDefault());
      case DECIMAL_SET_SCALE:
        return new DrillDecimalSetScaleFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic,
            registeredNames,
            ps, outputField, works, initializer, FunctionCostCategory.getDefault());
      case DECIMAL_ZERO_SCALE:
        return new DrillDecimalZeroScaleFuncHolder(scope, nullHandling, isBinaryCommutative, !isDeterministic,
            registeredNames,
            ps, outputField, works, initializer, FunctionCostCategory.getDefault());
      case HOLISTIC_AGGREGATE:
      case RANGE_AGGREGATE:
      default:
        return failure("Unsupported Function Type.", function);
      }
    } catch (Exception | NoSuchFieldError | AbstractMethodError ex) {
      return failure("Failure while creating function holder.", ex, function);
    }

  }




  @SuppressWarnings("unchecked")
  private <T> T getStaticFieldValue(String fieldName, Class<?> valueType, Class<T> c) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException{
      Field f = valueType.getDeclaredField(fieldName);
      Object val = f.get(null);
      return (T) val;
  }

  private static DrillFuncHolder failure(String message, Throwable t, FunctionDescriptor func, FieldDescriptor field) {
    logger.warn("Failure loading function class {}, field {}. " + message, func.getClassName(), field.getName(), t);
    return null;
  }

  private DrillFuncHolder failure(String message, FunctionDescriptor func, FieldDescriptor field) {
    logger.warn("Failure loading function class {}, field {}. " + message, func.getClassName(), field.getName());
    return null;
  }

  private DrillFuncHolder failure(String message, FunctionDescriptor func) {
    logger.warn("Failure loading function class [{}]. Message: {}", func.getClassName(), message);
    return null;
  }

  private DrillFuncHolder failure(String message, Throwable t, FunctionDescriptor func) {
    logger.warn("Failure loading function class [{}]. Message: {}", func.getClassName(), message, t);
    return null;
  }


}
