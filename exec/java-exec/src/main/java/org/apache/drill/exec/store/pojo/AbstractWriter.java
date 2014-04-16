package org.apache.drill.exec.store.pojo;

import java.lang.reflect.Field;

import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.ValueVector;

abstract class AbstractWriter<V extends ValueVector> implements PojoWriter{

  protected final Field field;
  protected V vector;
  protected final MajorType type;

  public AbstractWriter(Field field, MajorType type){
    this.field = field;
    this.type = type;
  }

  @Override
  public void init(OutputMutator output) throws SchemaChangeException {
    MaterializedField mf = MaterializedField.create(field.getName(), type);
    @SuppressWarnings("unchecked")
    Class<V> valueVectorClass = (Class<V>) TypeHelper.getValueVectorClass(type.getMinorType(), type.getMode());
    this.vector = output.addField(mf, valueVectorClass);
  }

  @Override
  public void allocate() {
    AllocationHelper.allocate(vector, 500, 100);
  }

  public void setValueCount(int valueCount){
    vector.getMutator().setValueCount(valueCount);
  }

  @Override
  public void cleanup() {
  }


}