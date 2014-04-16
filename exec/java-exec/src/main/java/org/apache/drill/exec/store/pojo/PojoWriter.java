package org.apache.drill.exec.store.pojo;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.physical.impl.OutputMutator;

interface PojoWriter{
  boolean writeField(Object pojo, int outboundIndex) throws IllegalArgumentException, IllegalAccessException ;
  void init(OutputMutator output) throws SchemaChangeException;
  void allocate();
  void setValueCount(int i);
  void cleanup();
}