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
package org.apache.drill.exec.store.phoenix;

import java.io.IOException;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableDateVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableTimeVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.execute.TupleProjector;
import org.apache.phoenix.iterate.ResultIterator;
import org.apache.phoenix.iterate.ScanningResultIterator;
import org.apache.phoenix.monitoring.CombinableMetric;
import org.apache.phoenix.schema.KeyValueSchema;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.ValueBitSet;
import org.apache.phoenix.schema.ValueSchema;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.util.SQLCloseable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

@SuppressWarnings("unchecked")
class PhoenixRecordReader extends AbstractRecordReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(PhoenixRecordReader.class);

  private static final ImmutableMap<Integer, MinorType> JDBC_TYPE_MAPPINGS;

  private final String storagePluginName;
  private final Scan scan;
  private final Connection connection;
  private final String tableName;

  private ResultIterator result;
  private KeyValueSchema kvSchema;
  private ImmutableList<ValueVector> vectors;
  private ImmutableList<Copier<?>> copiers;

  // workspace for each row
  private final ImmutableBytesWritable ptr = new ImmutableBytesWritable();

  public PhoenixRecordReader(FragmentContext fragmentContext, Connection connection, String tableName, Scan scan,
      String storagePluginName) {
    this.connection = connection;
    this.scan = scan;
    this.tableName = tableName;
    this.storagePluginName = storagePluginName;
  }

  static {
    JDBC_TYPE_MAPPINGS = (ImmutableMap<Integer, MinorType>) (Object) ImmutableMap.builder()
        .put(java.sql.Types.DOUBLE, MinorType.FLOAT8)
        .put(java.sql.Types.FLOAT, MinorType.FLOAT4)
        .put(java.sql.Types.TINYINT, MinorType.INT)
        .put(java.sql.Types.SMALLINT, MinorType.INT)
        .put(java.sql.Types.INTEGER, MinorType.INT)
        .put(java.sql.Types.BIGINT, MinorType.BIGINT)

        .put(java.sql.Types.CHAR, MinorType.VARCHAR)
        .put(java.sql.Types.VARCHAR, MinorType.VARCHAR)
        .put(java.sql.Types.LONGVARCHAR, MinorType.VARCHAR)

        .put(java.sql.Types.NCHAR, MinorType.VARCHAR)
        .put(java.sql.Types.NVARCHAR, MinorType.VARCHAR)
        .put(java.sql.Types.LONGNVARCHAR, MinorType.VARCHAR)

        .put(java.sql.Types.VARBINARY, MinorType.VARBINARY)
        .put(java.sql.Types.LONGVARBINARY, MinorType.VARBINARY)

        .put(java.sql.Types.NUMERIC, MinorType.FLOAT8)
        .put(java.sql.Types.DECIMAL, MinorType.FLOAT8)
        .put(java.sql.Types.REAL, MinorType.FLOAT8)

        .put(java.sql.Types.DATE, MinorType.DATE)
        .put(java.sql.Types.TIME, MinorType.TIME)
        .put(java.sql.Types.TIMESTAMP, MinorType.TIMESTAMP)

        .put(java.sql.Types.BOOLEAN, MinorType.BIT)

        .build();
  }

  private Copier<?> getCopier(int offset, ValueVector v) {
    if (v instanceof NullableBigIntVector) {
      return new BigIntCopier(offset, (NullableBigIntVector.Mutator) v.getMutator());
    } else if (v instanceof NullableFloat4Vector) {
      return new Float4Copier(offset, (NullableFloat4Vector.Mutator) v.getMutator());
    } else if (v instanceof NullableFloat8Vector) {
      return new Float8Copier(offset, (NullableFloat8Vector.Mutator) v.getMutator());
    } else if (v instanceof NullableIntVector) {
      return new IntCopier(offset, (NullableIntVector.Mutator) v.getMutator());
    } else if (v instanceof NullableVarCharVector) {
      return new VarCharCopier(offset, (NullableVarCharVector.Mutator) v.getMutator());
    } else if (v instanceof NullableVarBinaryVector) {
      return new VarBinaryCopier(offset, (NullableVarBinaryVector.Mutator) v.getMutator());
    } else if (v instanceof NullableDateVector) {
      return new DateCopier(offset, (NullableDateVector.Mutator) v.getMutator());
    } else if (v instanceof NullableTimeVector) {
      return new TimeCopier(offset, (NullableTimeVector.Mutator) v.getMutator());
    } else if (v instanceof NullableTimeStampVector) {
      return new TimeStampCopier(offset, (NullableTimeStampVector.Mutator) v.getMutator());
    } else if (v instanceof NullableBitVector) {
      return new BitCopier(offset, (NullableBitVector.Mutator) v.getMutator());
    }

    throw new IllegalArgumentException("Unknown how to handle vector.");
  }

  @Override
  public void setup(OperatorContext operatorContext, OutputMutator output) throws ExecutionSetupException {
    try {
      Table table = connection.getTable(TableName.valueOf(tableName));
      ResultScanner s = table.getScanner(scan);
      this.result = new ScanningResultIterator(s, CombinableMetric.NoOpRequestMetric.INSTANCE);
      this.kvSchema = TupleProjector.deserializeProjectorFromScan(scan).getSchema();
      String[] columnNames = PhoenixPrel.deserializeColumnInfoFromScan(scan);
      
      ImmutableList.Builder<ValueVector> vectorBuilder = ImmutableList.builder();
      ImmutableList.Builder<Copier<?>> copierBuilder = ImmutableList.builder();

      for (int i = 0; i < kvSchema.getFieldCount(); i++) {
        ValueSchema.Field phoenixField = kvSchema.getField(i);
        final PDataType phoenixType = phoenixField.getDataType();
        MinorType minorType = JDBC_TYPE_MAPPINGS.get(phoenixType.getSqlType());
        if (minorType == null) {
          throw UserException.dataReadError()
              .message(
                  "The JDBC storage plugin failed while trying to execute a query. "
                      + "The JDBC data type %d is not currently supported.",
                  phoenixType)
              .addContext("plugin", storagePluginName)
              .build(logger);
        }

        final MajorType type = Types.optional(minorType);
        final MaterializedField field = MaterializedField.create(columnNames[i], type);
        final Class<? extends ValueVector> clazz = (Class<? extends ValueVector>) TypeHelper.getValueVectorClass(
            minorType, type.getMode());
        ValueVector vector = output.addField(field, clazz);
        vectorBuilder.add(vector);
        copierBuilder.add(getCopier(i, vector));
      }

      vectors = vectorBuilder.build();
      copiers = copierBuilder.build();

    } catch (SchemaChangeException | IOException e) {
      throw UserException.dataReadError(e)
          .message("The JDBC storage plugin failed while trying setup the SQL query. ")
          .addContext("table", tableName)
          .addContext("plugin", storagePluginName)
          .build(logger);
    }
  }

  @Override
  public int next() {
    int counter = 0;
    try {
      final ValueBitSet valueSet = ValueBitSet.newInstance(kvSchema);
      for (;;) {
        Tuple tuple = result.next();
        if (tuple == null) {
          break;
        }

        final Cell value = tuple.getValue(0);
        ptr.set(value.getValueArray(),
            value.getValueOffset(),
            value.getValueLength());
        valueSet.clear();
        valueSet.or(ptr);

        final int maxOffset = ptr.getOffset() + ptr.getLength();
        kvSchema.iterator(ptr);
        for (int i = 0;; i++) {
          final Boolean hasValue = kvSchema.next(ptr, i, maxOffset, valueSet);
          if (hasValue == null) {
            break;
          }
          final Copier<?> copier = copiers.get(i);
          if (hasValue) {
            copier.copy(counter);
          }
        }
        if (++counter == 4095) {
          // loop at 4095 since nullables use one more than record count and we
          // allocate on powers of two.
          break;
        }
      }
    } catch (Exception e) {
      throw UserException
          .dataReadError(e)
          .message("Failure while attempting to read from database.")
          .addContext("table", tableName)
          .addContext("plugin", storagePluginName)
          .build(logger);
    }

    for (ValueVector vv : vectors) {
      vv.getMutator().setValueCount(counter > 0 ? counter : 0);
    }

    return counter > 0 ? counter : 0;
  }

  @Override
  public void cleanup() {
    close(result, logger);
  }

  /** Similar to {@link org.apache.drill.common.AutoCloseables#close}. */
  public static void close(final SQLCloseable ac, final org.slf4j.Logger logger) {
    if (ac == null) {
      return;
    }

    try {
      ac.close();
    } catch (Exception e) {
      logger.warn("Failure on close(): {}", e);
    }
  }

  private abstract class Copier<T extends ValueVector.Mutator> {
    protected final int columnIndex;
    protected final PDataType.PDataCodec codec;
    protected final SortOrder sortOrder;
    protected final T mutator;

    public Copier(int columnIndex, T mutator) {
      super();
      this.columnIndex = columnIndex;
      final ValueSchema.Field field = kvSchema.getField(columnIndex);
      this.codec = field.getDataType().getCodec();
      this.mutator = mutator;
      this.sortOrder = field.getSortOrder();
    }

    abstract void copy(int index);
  }

  private class IntCopier extends Copier<NullableIntVector.Mutator> {
    public IntCopier(int offset, NullableIntVector.Mutator mutator) {
      super(offset, mutator);
    }

    @Override
    void copy(int index) {
      mutator.setSafe(index, codec.decodeInt(ptr, sortOrder));
    }
  }

  private class BigIntCopier extends Copier<NullableBigIntVector.Mutator> {
    public BigIntCopier(int offset, NullableBigIntVector.Mutator mutator) {
      super(offset, mutator);
    }

    @Override
    void copy(int index) {
      mutator.setSafe(index, codec.decodeLong(ptr, sortOrder));
    }

  }

  private class Float4Copier extends Copier<NullableFloat4Vector.Mutator> {

    public Float4Copier(int columnIndex, NullableFloat4Vector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(int index) {
      mutator.setSafe(index, codec.decodeFloat(ptr, sortOrder));
    }

  }

  private class Float8Copier extends Copier<NullableFloat8Vector.Mutator> {

    public Float8Copier(int columnIndex, NullableFloat8Vector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(int index) {
      mutator.setSafe(index, codec.decodeDouble(ptr, sortOrder));
    }

  }

  private class VarCharCopier extends Copier<NullableVarCharVector.Mutator> {

    public VarCharCopier(int columnIndex, NullableVarCharVector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(int index) {
      mutator.setSafe(index, ptr.get(), ptr.getOffset(), ptr.getLength());
    }

  }

  private class VarBinaryCopier extends Copier<NullableVarBinaryVector.Mutator> {

    public VarBinaryCopier(int columnIndex, NullableVarBinaryVector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(int index) {
      mutator.setSafe(index, ptr.get(), ptr.getOffset(), ptr.getLength());
    }

  }

  private class DateCopier extends Copier<NullableDateVector.Mutator> {

    public DateCopier(int columnIndex, NullableDateVector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(int index) {
      mutator.setSafe(index, codec.decodeLong(ptr, sortOrder));
    }

  }

  private class TimeCopier extends Copier<NullableTimeVector.Mutator> {

    public TimeCopier(int columnIndex, NullableTimeVector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(int index) {
      mutator.setSafe(index, codec.decodeInt(ptr, sortOrder));
    }

  }

  private class TimeStampCopier extends Copier<NullableTimeStampVector.Mutator> {

    public TimeStampCopier(int columnIndex, NullableTimeStampVector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(int index) {
      mutator.setSafe(index, codec.decodeLong(ptr, sortOrder));
    }

  }

  private class BitCopier extends Copier<NullableBitVector.Mutator> {

    public BitCopier(int columnIndex, NullableBitVector.Mutator mutator) {
      super(columnIndex, mutator);
    }

    @Override
    void copy(int index) {
      mutator.setSafe(index, codec.decodeByte(ptr, sortOrder));
    }

  }

}