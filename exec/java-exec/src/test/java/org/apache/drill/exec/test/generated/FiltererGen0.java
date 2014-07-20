package org.apache.drill.exec.test.generated;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueHolderHelper;

public class FiltererGen0 {
  NullableVarCharVector vv0;
  VarCharHolder string4;
  VarCharHolder constant5;

  public void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing) throws SchemaChangeException {
    {
      int[] fieldIds1 = new int[1];
      fieldIds1[0] = 0;
      Object tmp2 = (incoming).getValueAccessorById(NullableVarCharVector.class, fieldIds1).getValueVector();
      if (tmp2 == null) {
        throw new SchemaChangeException(
            "Failure while loading vector vv0 with id: org.apache.drill.exec.record.TypedFieldId@283c3e27.");
      }
      vv0 = ((NullableVarCharVector) tmp2);
      string4 = ValueHolderHelper.getVarCharHolder("James Compagno");
      constant5 = string4;
      /** start SETUP for function equal **/
      {
        VarCharHolder right = constant5;
        {
        }
      }
      /** end SETUP for function equal **/
    }
  }

  public boolean doEval(int inIndex, int outIndex) throws SchemaChangeException {
    {
      NullableVarCharHolder out3 = new NullableVarCharHolder();
      out3.isSet = vv0.getAccessor().isSet((inIndex));
      if (out3.isSet == 1) {
        {
          out3.buffer = vv0.getData();
          long startEnd = vv0.getAccessor().getStartEnd(inIndex);
          out3.start = (int) (startEnd >> 32);
          out3.end = (int) (startEnd);
//          vv0.getAccessor().get((inIndex), out3);
        }
      }
      // ---- start of eval portion of equal function. ----//
      NullableBitHolder out6 = new NullableBitHolder();
      {
        if (out3.isSet == 0) {
          out6.isSet = 0;
        } else {
          final NullableBitHolder out = new NullableBitHolder();
          NullableVarCharHolder left = out3;
          VarCharHolder right = constant5;
          GCompareVarCharVarChar$EqualsVarCharVarChar_eval: {
            outside: {
              if (left.end - left.start == right.end - right.start) {
                int n = left.end - left.start;
                int l = left.start;
                int r = right.start;
                while (n-- != 0) {
                  byte leftByte = left.buffer.getByte(l++);
                  byte rightByte = right.buffer.getByte(r++);
                  if (leftByte != rightByte) {
                    out.value = 0;
                    break outside;
                  }
                }
                out.value = 1;
              } else {
                out.value = 0;
              }
            }
          }
          out.isSet = 1;
          out6 = out;
          out.isSet = 1;
        }
      }
      // ---- end of eval portion of equal function. ----//
      return (out6.value == 1);
    }
  }
}
