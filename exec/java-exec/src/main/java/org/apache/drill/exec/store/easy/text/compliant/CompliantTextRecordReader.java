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
package org.apache.drill.exec.store.easy.text.compliant;

import io.netty.buffer.DrillBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.vector.RepeatedVarCharVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileSplit;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public class CompliantTextRecordReader extends AbstractRecordReader implements AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompliantTextRecordReader.class);

  private static final String COL_NAME = "columns";
  private static final FieldReference ref = new FieldReference(COL_NAME);
  private static final SchemaPath COLUMNS = SchemaPath.getSimplePath("columns");

  private TextParsingSettings settings;
  private RepeatedVarCharVector vector;
  private final FragmentContext context;
  private FileSplit split;
  private List<Integer> columnIds = new ArrayList<Integer>();
  private TextReader reader;
  private DrillBuf readBuffer;
  private DrillBuf whitespaceBuffer;

  public CompliantTextRecordReader(FileSplit split, FragmentContext context, char delimiter, List<SchemaPath> columns) {
    this.context = context;
    this.split = split;
    this.settings = new TextParsingSettings();
    settings.setDelimiter((byte) delimiter);
    setColumns(columns);

    if (!isStarQuery()) {
      String pathStr;
      for (SchemaPath path : columns) {
        assert path.getRootSegment().isNamed();
        pathStr = path.getRootSegment().getPath();
        Preconditions.checkArgument(pathStr.equals(COL_NAME) || (pathStr.equals("*") && path.getRootSegment().getChild() == null),
            "Selected column(s) must have name 'columns' or must be plain '*'");

        if (path.getRootSegment().getChild() != null) {
          Preconditions.checkArgument(path.getRootSegment().getChild().isArray(), "Selected column must be an array index");
          int index = path.getRootSegment().getChild().getArraySegment().getIndex();
          columnIds.add(index);
        }
      }
      Collections.sort(columnIds);
    }
  }

  @Override
  public boolean isStarQuery() {
    return super.isStarQuery() || Iterables.tryFind(getColumns(), new Predicate<SchemaPath>() {
      @Override
      public boolean apply(@Nullable SchemaPath path) {
        return path.equals(COLUMNS);
      }
    }).isPresent();
  }

  @Override
  public void setup(OperatorContext context, OutputMutator outputMutator) throws ExecutionSetupException {
    MaterializedField field = MaterializedField.create(ref, Types.repeated(TypeProtos.MinorType.VARCHAR));

    readBuffer = context.getManagedBuffer(1024*1024);
    whitespaceBuffer = context.getManagedBuffer(64*1024);

    try {
      vector = outputMutator.addField(field, RepeatedVarCharVector.class);
      FileSystem fs = split.getPath().getFileSystem(new Configuration());
      FSDataInputStream stream = fs.open(split.getPath());

      TextInput input = new TextInput(context.getStats(), settings.getNewLineDelimiter(), settings.getNormalizedNewLine(),  stream, readBuffer, split.getStart(), split.getStart() + split.getLength());
      boolean[] fields = new boolean[1000];

      int maxField = fields.length;

      if(this.isStarQuery()){
        Arrays.fill(fields, true);
      }else{
        for(Integer i : columnIds){
          maxField = 0;
          maxField = Math.max(maxField, i);
          fields[i] = true;
        }
      }

      RepeatedVarCharOutput output = new RepeatedVarCharOutput(vector, fields, maxField);
      this.reader = new TextReader(new TextParsingSettings(), input, output, whitespaceBuffer);
      reader.start();
    } catch (SchemaChangeException | IOException e) {
      throw new ExecutionSetupException(e);
    }
  }

  @Override
  public int next() {
    reader.resetForNextBatch();
    int cnt = 0;

    try{
      while(cnt < 5001 && reader.parseNext()){
        cnt++;
      }
      reader.finishBatch();
      return cnt;
    }catch(IOException e){
      throw new DrillRuntimeException(e);
    }
  }

  @Override
  public void cleanup() {
    try {
      reader.close();
    } catch (IOException e) {
      logger.warn("Exception while closing stream.", e);
    }
  }

  @Override
  public void close() {
    cleanup();
  }
}
