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
import io.netty.util.internal.PlatformDependent;

import org.apache.drill.exec.vector.RepeatedVarCharVector;

public class RepeatedVarCharOutput extends TextOutput {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RepeatedVarCharOutput.class);

  private RepeatedVarCharVector.Mutator mutator;
  private RepeatedVarCharVector vector;
  private boolean[] collectedFields;

  private long repeatedOffset;
  private long repeatedOffsetMax;

  private long characterDataOriginal;
  private long characterData;
  private long characterDataMax;

  private long charLengthOffsetOriginal;
  private long charLengthOffset;
  private long charLengthOffsetMax;

  private boolean ok;

  private boolean decrementMonitor = true;

  private long recordCount;
  private int fieldIndex = -1;
  private int batchIndex;
  private boolean collect;
  private boolean fieldOpen;
  private final int maxField;

  public RepeatedVarCharOutput(RepeatedVarCharVector vector, boolean[] collectedFields, int maxField) {
    super();
    this.vector = vector;
    this.mutator = vector.getMutator();
    this.collectedFields = collectedFields;
    this.maxField = maxField;
  }

  public void startBatch() {
    this.fieldOpen = false;
    this.batchIndex = 0;
    this.fieldIndex = -1;
    this.collect = true;
    this.decrementMonitor = true;

    this.ok = true;

    { // repeated offset
      DrillBuf buf =  vector.getOffsetVector().getData();
      checkBuf(buf);
      this.repeatedOffset = buf.memoryAddress() + 4;
      this.repeatedOffsetMax = buf.memoryAddress() + buf.capacity();
    }

    { // character data
      DrillBuf buf =  vector.getValuesVector().getData();
      checkBuf(buf);
      this.characterData = buf.memoryAddress();
      this.characterDataOriginal = buf.memoryAddress();
      this.characterDataMax = buf.memoryAddress() + buf.capacity();
    }

    { // character length
      DrillBuf buf =  vector.getValuesVector().getOffsetVector().getData();
      checkBuf(buf);
      this.charLengthOffset = buf.memoryAddress() + 4;
      this.charLengthOffsetOriginal = buf.memoryAddress() + 4; // add four as offsets conceptually start at 1. (first item is 0..1)
      this.charLengthOffsetMax = buf.memoryAddress() + buf.capacity();
    }

  }

  private void checkBuf(DrillBuf b){
    if(b.refCnt() < 1){
      throw new IllegalStateException("Cannot access a dereferenced buffer.");
    }
  }

  @Override
  public void startField(int index) {
    fieldIndex = index;
    collect = collectedFields[index];
    fieldOpen = true;
  }


  @Override
  public boolean endField() {
    fieldOpen = false;

    if(charLengthOffset < charLengthOffsetMax){
      int newOffset = (int) (characterData - characterDataOriginal);
      PlatformDependent.putInt(charLengthOffset, newOffset);
      charLengthOffset += 4;
      return fieldIndex < maxField;
    }else{
      if(decrementMonitor){
        vector.getValuesVector().getOffsetVector().decrementAllocationMonitor();
        decrementMonitor = false;
      }
      ok = false;
      return false;
    }

  }

  @Override
  public boolean endEmptyField() {
    return endField();
  }

  @Override
  public void append(byte data) {
    if(!collect){
      return;
    }else if(characterData < characterDataMax){
      PlatformDependent.putByte(characterData, data);
      characterData++;
    }else{
      if(decrementMonitor){
        vector.getValuesVector().decrementAllocationMonitor();
        decrementMonitor = false;
      }
      collect = false;
      ok = false;
    }
  }


  @Override
  public long getRecordCount() {
    return recordCount;
  }

  @Override
  public boolean finishRecord() {

    if(fieldOpen){
      endField();
    }

    if(repeatedOffset < repeatedOffsetMax){
      int newOffset = ((int) (charLengthOffset - charLengthOffsetOriginal))/4;
      PlatformDependent.putInt(repeatedOffset, newOffset);
      repeatedOffset += 4;

      // if there were no defined fields, skip.
      if(fieldIndex > -1){
        batchIndex++;
        recordCount++;
      }
    }else{
      if(decrementMonitor){
        vector.getOffsetVector().decrementAllocationMonitor();
        decrementMonitor = false;
      }
      ok = false;
    }

    return ok;

  }


  @Override
  public void finishBatch() {
    mutator.setValueCount(batchIndex);
  }



}
