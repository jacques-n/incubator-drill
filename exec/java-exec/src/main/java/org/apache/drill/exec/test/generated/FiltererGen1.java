      package org.apache.drill.exec.test.generated;

      import org.apache.drill.exec.exception.SchemaChangeException;
      import org.apache.drill.exec.expr.holders.BitHolder;
      import org.apache.drill.exec.expr.holders.DateHolder;
      import org.apache.drill.exec.expr.holders.IntHolder;
      import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
      import org.apache.drill.exec.expr.holders.NullableBitHolder;
     import org.apache.drill.exec.expr.holders.NullableDateHolder;
     import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
     import org.apache.drill.exec.expr.holders.VarCharHolder;
     import org.apache.drill.exec.ops.FragmentContext;
     import org.apache.drill.exec.record.RecordBatch;
     import org.apache.drill.exec.vector.NullableBigIntVector;
     import org.apache.drill.exec.vector.NullableBitVector;
     import org.apache.drill.exec.vector.NullableVarCharVector;
     import org.apache.drill.exec.vector.ValueHolderHelper;
     import org.joda.time.MutableDateTime;

     public class FiltererGen1 {

         NullableBitVector vv1;
         BitHolder constant6;
         NullableBigIntVector vv9;
         IntHolder constant14;
         NullableBigIntVector vv16;
         IntHolder constant21;
         NullableBigIntVector vv23;
         IntHolder constant28;
         NullableBigIntVector vv30;
         IntHolder constant35;
         NullableVarCharVector vv37;
         MutableDateTime work42;
         long work43;
         DateHolder constant47;
         NullableVarCharVector vv49;
         NullableBigIntVector vv55;
         IntHolder constant60;
         VarCharHolder string62;
         VarCharHolder constant63;
         NullableBigIntVector vv65;
         IntHolder constant70;
         VarCharHolder string72;
         VarCharHolder constant73;
         NullableBigIntVector vv75;
         IntHolder constant80;
         VarCharHolder string82;
         VarCharHolder constant83;
         NullableBigIntVector vv85;
         IntHolder constant90;
         VarCharHolder string92;
         VarCharHolder constant93;
         VarCharHolder string94;
         VarCharHolder constant95;

         public void doSetup(FragmentContext context, RecordBatch incoming, RecordBatch outgoing)
             throws SchemaChangeException
         {
             {
                 int[] fieldIds2 = new int[ 1 ] ;
                 fieldIds2 [ 0 ] = 2;
                 Object tmp3 = (incoming).getValueAccessorById(NullableBitVector.class, fieldIds2).getValueVector();
                 if (tmp3 == null) {
                     throw new SchemaChangeException("Failure while loading vector vv1 with id: TypedFieldId [fieldIds=[2], remainder=null].");
                 }
                 vv1 = ((NullableBitVector) tmp3);
                 BitHolder out5 = new BitHolder();
                 out5 .value = 1;
                 constant6 = out5;
                 /** start SETUP for function equal **/
                 {
                     BitHolder right = constant6;
                      {}
                 }
                 /** end SETUP for function equal **/
                 int[] fieldIds10 = new int[ 1 ] ;
                 fieldIds10 [ 0 ] = 1;
                 Object tmp11 = (incoming).getValueAccessorById(NullableBigIntVector.class, fieldIds10).getValueVector();
                 if (tmp11 == null) {
                     throw new SchemaChangeException("Failure while loading vector vv9 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                 }
                 vv9 = ((NullableBigIntVector) tmp11);
                 IntHolder out13 = new IntHolder();
                 out13 .value = 2726;
                 constant14 = out13;
                 /** start SETUP for function equal **/
                 {
                     IntHolder right = constant14;
                      {}
                 }
                 /** end SETUP for function equal **/
                 int[] fieldIds17 = new int[ 1 ] ;
                 fieldIds17 [ 0 ] = 1;
                 Object tmp18 = (incoming).getValueAccessorById(NullableBigIntVector.class, fieldIds17).getValueVector();
                 if (tmp18 == null) {
                     throw new SchemaChangeException("Failure while loading vector vv16 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                 }
                 vv16 = ((NullableBigIntVector) tmp18);
                IntHolder out20 = new IntHolder();
                out20 .value = 2252;
                constant21 = out20;
                /** start SETUP for function equal **/
                {
                    IntHolder right = constant21;
                     {}
                }
                /** end SETUP for function equal **/
                int[] fieldIds24 = new int[ 1 ] ;
                fieldIds24 [ 0 ] = 1;
                Object tmp25 = (incoming).getValueAccessorById(NullableBigIntVector.class, fieldIds24).getValueVector();
                if (tmp25 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv23 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv23 = ((NullableBigIntVector) tmp25);
                IntHolder out27 = new IntHolder();
                out27 .value = 2755;
                constant28 = out27;
                /** start SETUP for function equal **/
                {
                    IntHolder right = constant28;
                     {}
                }
                /** end SETUP for function equal **/
                int[] fieldIds31 = new int[ 1 ] ;
                fieldIds31 [ 0 ] = 1;
                Object tmp32 = (incoming).getValueAccessorById(NullableBigIntVector.class, fieldIds31).getValueVector();
                if (tmp32 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv30 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv30 = ((NullableBigIntVector) tmp32);
                IntHolder out34 = new IntHolder();
                out34 .value = 3931;
                constant35 = out34;
                /** start SETUP for function equal **/
                {
                    IntHolder right = constant35;
                     {}
                }
                /** end SETUP for function equal **/
                int[] fieldIds38 = new int[ 1 ] ;
                fieldIds38 [ 0 ] = 0;
                Object tmp39 = (incoming).getValueAccessorById(NullableVarCharVector.class, fieldIds38).getValueVector();
                if (tmp39 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv37 with id: TypedFieldId [fieldIds=[0], remainder=null].");
                }
                vv37 = ((NullableVarCharVector) tmp39);
                /** start SETUP for function castDATE **/
                {
                     {}
                }
                /** end SETUP for function castDATE **/
                /** start SETUP for function current_date **/
                {
                    long queryStartDate = work43;

    DateTypeFunctions$CurrentDate_setup: {
        int timeZoneIndex = incoming.getContext().getRootFragmentTimeZone();
        org.joda.time.DateTimeZone timeZone = org.joda.time.DateTimeZone.forID(org.apache.drill.exec.expr.fn.impl.DateUtility.getTimeZone(timeZoneIndex));
        org.joda.time.DateTime now = new org.joda.time.DateTime(incoming.getContext().getQueryStartTime(), timeZone);

        queryStartDate = (new org.joda.time.DateMidnight(now.getYear(), now.getMonthOfYear(), now.getDayOfMonth(), timeZone)).getMillis();
    }

                    work43 = queryStartDate;
                }
                /** end SETUP for function current_date **/
                //---- start of eval portion of current_date function. ----//
                DateHolder out44 = new DateHolder();
                {
                    final DateHolder out = new DateHolder();
                    long queryStartDate = work43;

    DateTypeFunctions$CurrentDate_eval: {
        out.value = queryStartDate;
    }

                    work43 = queryStartDate;
                    out44 = out;
                }
                //---- end of eval portion of current_date function. ----//
                IntHolder out45 = new IntHolder();
                out45 .value = 15;
                /** start SETUP for function date_sub **/
                {
                    DateHolder left = out44;
                    IntHolder right = out45;
                    MutableDateTime temp = work42;

    DateIntFunctions$DateIntSubtractFunction_setup: {
        temp = new org.joda.time.MutableDateTime(org.joda.time.DateTimeZone.UTC);
    }

                    work42 = temp;
                }
                /** end SETUP for function date_sub **/
                //---- start of eval portion of date_sub function. ----//
                DateHolder out46 = new DateHolder();
                {
                    final DateHolder out = new DateHolder();
                    DateHolder left = out44;
                    IntHolder right = out45;
                    MutableDateTime temp = work42;

    DateIntFunctions$DateIntSubtractFunction_eval: {
        temp.setMillis(left.value);
        temp.addDays((int) right.value * -1);
        out.value = temp.getMillis();
    }

                    work42 = temp;
                    out46 = out;
                }
                //---- end of eval portion of date_sub function. ----//
                constant47 = out46;
                /** start SETUP for function less_than_or_equal_to **/
                {
                    DateHolder right = constant47;
                     {}
                }
                /** end SETUP for function less_than_or_equal_to **/
                int[] fieldIds50 = new int[ 1 ] ;
                fieldIds50 [ 0 ] = 0;
                Object tmp51 = (incoming).getValueAccessorById(NullableVarCharVector.class, fieldIds50).getValueVector();
                if (tmp51 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv49 with id: TypedFieldId [fieldIds=[0], remainder=null].");
                }
                vv49 = ((NullableVarCharVector) tmp51);
                /** start SETUP for function castDATE **/
                {
                     {}
                }
                /** end SETUP for function castDATE **/
                int[] fieldIds56 = new int[ 1 ] ;
                fieldIds56 [ 0 ] = 1;
                Object tmp57 = (incoming).getValueAccessorById(NullableBigIntVector.class, fieldIds56).getValueVector();
                if (tmp57 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv55 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv55 = ((NullableBigIntVector) tmp57);
                IntHolder out59 = new IntHolder();
                out59 .value = 2726;
                constant60 = out59;
                /** start SETUP for function equal **/
                {
                    IntHolder right = constant60;
                     {}
                }
                /** end SETUP for function equal **/
                string62 = ValueHolderHelper.getVarCharHolder((incoming).getContext().getManagedBuffer(), "2012-01-19");
                constant63 = string62;
                int[] fieldIds66 = new int[ 1 ] ;
                fieldIds66 [ 0 ] = 1;
                Object tmp67 = (incoming).getValueAccessorById(NullableBigIntVector.class, fieldIds66).getValueVector();
                if (tmp67 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv65 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv65 = ((NullableBigIntVector) tmp67);
                IntHolder out69 = new IntHolder();
                out69 .value = 2252;
                constant70 = out69;
                /** start SETUP for function equal **/
                {
                    IntHolder right = constant70;
                     {}
                }
                /** end SETUP for function equal **/
                string72 = ValueHolderHelper.getVarCharHolder((incoming).getContext().getManagedBuffer(), "2012-01-19");
                constant73 = string72;
                int[] fieldIds76 = new int[ 1 ] ;
                fieldIds76 [ 0 ] = 1;
                Object tmp77 = (incoming).getValueAccessorById(NullableBigIntVector.class, fieldIds76).getValueVector();
                if (tmp77 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv75 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv75 = ((NullableBigIntVector) tmp77);
                IntHolder out79 = new IntHolder();
                out79 .value = 2755;
                constant80 = out79;
                /** start SETUP for function equal **/
                {
                    IntHolder right = constant80;
                     {}
                }
                /** end SETUP for function equal **/
                string82 = ValueHolderHelper.getVarCharHolder((incoming).getContext().getManagedBuffer(), "2012-11-22");
                constant83 = string82;
                int[] fieldIds86 = new int[ 1 ] ;
                fieldIds86 [ 0 ] = 1;
                Object tmp87 = (incoming).getValueAccessorById(NullableBigIntVector.class, fieldIds86).getValueVector();
                if (tmp87 == null) {
                    throw new SchemaChangeException("Failure while loading vector vv85 with id: TypedFieldId [fieldIds=[1], remainder=null].");
                }
                vv85 = ((NullableBigIntVector) tmp87);
                IntHolder out89 = new IntHolder();
                out89 .value = 3931;
                constant90 = out89;
                /** start SETUP for function equal **/
                {
                    IntHolder right = constant90;
                     {}
                }
                /** end SETUP for function equal **/
                string92 = ValueHolderHelper.getVarCharHolder((incoming).getContext().getManagedBuffer(), "2013-06-25");
                constant93 = string92;
                string94 = ValueHolderHelper.getVarCharHolder((incoming).getContext().getManagedBuffer(), "9999-12-31");
                constant95 = string94;
                /** start SETUP for function castDATE **/
                {
                     {}
                }
                /** end SETUP for function castDATE **/
                /** start SETUP for function greater_than_or_equal_to **/
                {
                     {}
                }
                /** end SETUP for function greater_than_or_equal_to **/
            }
        }

        public boolean doEval(int inIndex, int outIndex)
            throws SchemaChangeException
        {
            {
                NullableBitHolder out0 = new NullableBitHolder();
                AndOP0:
                {
                    NullableBitHolder out4 = new NullableBitHolder();
                    {
                        out4 .isSet = vv1 .getAccessor().isSet((inIndex));
                        if (out4 .isSet == 1) {
                            out4 .value = vv1 .getAccessor().get((inIndex));
                        }
                    }
                    //---- start of eval portion of equal function. ----//
                    NullableBitHolder out7 = new NullableBitHolder();
                    {
                        if (out4 .isSet == 0) {
                            out7 .isSet = 0;
                        } else {
                            final NullableBitHolder out = new NullableBitHolder();
                            NullableBitHolder left = out4;
                            BitHolder right = constant6;

    BitFunctions$EqualsBitBit_eval: {
        out.value = left.value == right.value ? 1 : 0;
    }

                            out.isSet = 1;
                            out7 = out;
                            out.isSet = 1;
                        }
                    }
                    //---- end of eval portion of equal function. ----//
                    if ((out7 .isSet == 1)&&(out7 .value!= 1)) {
                        out0 .isSet = 1;
                        out0 .value = 0;
                        break AndOP0;
                    }
                    NullableBitHolder out8 = new NullableBitHolder();
                    OrOP1:
                    {
                        NullableBigIntHolder out12 = new NullableBigIntHolder();
                        {
                            out12 .isSet = vv9 .getAccessor().isSet((inIndex));
                            if (out12 .isSet == 1) {
                                out12 .value = vv9 .getAccessor().get((inIndex));
                            }
                        }
                        //---- start of eval portion of equal function. ----//
                        NullableBitHolder out15 = new NullableBitHolder();
                        {
                            if (out12 .isSet == 0) {
                                out15 .isSet = 0;
                            } else {
                                final NullableBitHolder out = new NullableBitHolder();
                                NullableBigIntHolder left = out12;
                                IntHolder right = constant14;

    GCompareBigIntInt$EqualsBigIntInt_eval: {
        out.value = left.value == right.value ? 1 : 0;
    }

                                out.isSet = 1;
                                out15 = out;
                                out.isSet = 1;
                            }
                        }
                        //---- end of eval portion of equal function. ----//
                        if ((out15 .isSet == 1)&&(out15 .value == 1)) {
                            out8 .isSet = 1;
                            out8 .value = 1;
                            break OrOP1;
                        }
                        NullableBigIntHolder out19 = new NullableBigIntHolder();
                        {
                            out19 .isSet = vv16 .getAccessor().isSet((inIndex));
                            if (out19 .isSet == 1) {
                                out19 .value = vv16 .getAccessor().get((inIndex));
                            }
                        }
                        //---- start of eval portion of equal function. ----//
                        NullableBitHolder out22 = new NullableBitHolder();
                        {
                            if (out19 .isSet == 0) {
                                out22 .isSet = 0;
                            } else {
                                final NullableBitHolder out = new NullableBitHolder();
                                NullableBigIntHolder left = out19;
                                IntHolder right = constant21;

    GCompareBigIntInt$EqualsBigIntInt_eval: {
        out.value = left.value == right.value ? 1 : 0;
    }

                                out.isSet = 1;
                                out22 = out;
                                out.isSet = 1;
                            }
                        }
                        //---- end of eval portion of equal function. ----//
                        if ((out22 .isSet == 1)&&(out22 .value == 1)) {
                            out8 .isSet = 1;
                            out8 .value = 1;
                            break OrOP1;
                        }
                        NullableBigIntHolder out26 = new NullableBigIntHolder();
                        {
                            out26 .isSet = vv23 .getAccessor().isSet((inIndex));
                            if (out26 .isSet == 1) {
                                out26 .value = vv23 .getAccessor().get((inIndex));
                            }
                        }
                        //---- start of eval portion of equal function. ----//
                        NullableBitHolder out29 = new NullableBitHolder();
                        {
                            if (out26 .isSet == 0) {
                                out29 .isSet = 0;
                            } else {
                                final NullableBitHolder out = new NullableBitHolder();
                                NullableBigIntHolder left = out26;
                                IntHolder right = constant28;

    GCompareBigIntInt$EqualsBigIntInt_eval: {
        out.value = left.value == right.value ? 1 : 0;
    }

                                out.isSet = 1;
                                out29 = out;
                                out.isSet = 1;
                            }
                        }
                        //---- end of eval portion of equal function. ----//
                        if ((out29 .isSet == 1)&&(out29 .value == 1)) {
                            out8 .isSet = 1;
                            out8 .value = 1;
                            break OrOP1;
                        }
                        NullableBigIntHolder out33 = new NullableBigIntHolder();
                        {
                            out33 .isSet = vv30 .getAccessor().isSet((inIndex));
                            if (out33 .isSet == 1) {
                                out33 .value = vv30 .getAccessor().get((inIndex));
                            }
                        }
                        //---- start of eval portion of equal function. ----//
                        NullableBitHolder out36 = new NullableBitHolder();
                        {
                            if (out33 .isSet == 0) {
                                out36 .isSet = 0;
                            } else {
                                final NullableBitHolder out = new NullableBitHolder();
                                NullableBigIntHolder left = out33;
                                IntHolder right = constant35;

    GCompareBigIntInt$EqualsBigIntInt_eval: {
        out.value = left.value == right.value ? 1 : 0;
    }

                                out.isSet = 1;
                                out36 = out;
                                out.isSet = 1;
                            }
                        }
                        //---- end of eval portion of equal function. ----//
                        if ((out36 .isSet == 1)&&(out36 .value == 1)) {
                            out8 .isSet = 1;
                            out8 .value = 1;
                            break OrOP1;
                        }
                        if ((((out15 .isSet*out22 .isSet)*out29 .isSet)*out36 .isSet) == 0) {
                            out8 .isSet = 0;
                        } else {
                            out8 .isSet = 1;
                            out8 .value = 0;
                        }
                    }
                    if ((out8 .isSet == 1)&&(out8 .value!= 1)) {
                        out0 .isSet = 1;
                        out0 .value = 0;
                        break AndOP0;
                    }
                    NullableVarCharHolder out40 = new NullableVarCharHolder();
                    {
                        out40 .isSet = vv37 .getAccessor().isSet((inIndex));
                        if (out40 .isSet == 1) {
                            out40 .buffer = vv37 .getData();
                            long startEnd = vv37 .getAccessor().getStartEnd((inIndex));
                            out40 .start = ((int) startEnd);
                            out40 .end = ((int)(startEnd >> 32));
                        }
                    }
                    //---- start of eval portion of castDATE function. ----//
                    NullableDateHolder out41 = new NullableDateHolder();
                    {
                        if (out40 .isSet == 0) {
                            out41 .isSet = 0;
                        } else {
                            final NullableDateHolder out = new NullableDateHolder();
                            NullableVarCharHolder in = out40;

    CastVarCharToDate_eval: {
        out.value = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getDate(in.buffer, in.start, in.end);
    }

                            out.isSet = 1;
                            out41 = out;
                            out.isSet = 1;
                        }
                    }
                    //---- end of eval portion of castDATE function. ----//
                    //---- start of eval portion of less_than_or_equal_to function. ----//
                    NullableBitHolder out48 = new NullableBitHolder();
                    {
                        if (out41 .isSet == 0) {
                            out48 .isSet = 0;
                        } else {
                            final NullableBitHolder out = new NullableBitHolder();
                            NullableDateHolder left = out41;
                            DateHolder right = constant47;

    GCompareDateFunctions$LessThanEDate_eval: {
        out.value = left.value <= right.value ? 1 : 0;
    }

                            out.isSet = 1;
                            out48 = out;
                            out.isSet = 1;
                        }
                    }
                    //---- end of eval portion of less_than_or_equal_to function. ----//
                    if ((out48 .isSet == 1)&&(out48 .value!= 1)) {
                        out0 .isSet = 1;
                        out0 .value = 0;
                        break AndOP0;
                    }
                    NullableVarCharHolder out52 = new NullableVarCharHolder();
                    {
                        out52 .isSet = vv49 .getAccessor().isSet((inIndex));
                        if (out52 .isSet == 1) {
                            out52 .buffer = vv49 .getData();
                            long startEnd = vv49 .getAccessor().getStartEnd((inIndex));
                            out52 .start = ((int) startEnd);
                            out52 .end = ((int)(startEnd >> 32));
                        }
                    }
                    //---- start of eval portion of castDATE function. ----//
                    NullableDateHolder out53 = new NullableDateHolder();
                    {
                        if (out52 .isSet == 0) {
                            out53 .isSet = 0;
                        } else {
                            final NullableDateHolder out = new NullableDateHolder();
                            NullableVarCharHolder in = out52;

    CastVarCharToDate_eval: {
        out.value = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getDate(in.buffer, in.start, in.end);
    }

                            out.isSet = 1;
                            out53 = out;
                            out.isSet = 1;
                        }
                    }
                    //---- end of eval portion of castDATE function. ----//
                    VarCharHolder out54 = new VarCharHolder();
                    NullableBigIntHolder out58 = new NullableBigIntHolder();
                    {
                        out58 .isSet = vv55 .getAccessor().isSet((inIndex));
                        if (out58 .isSet == 1) {
                            out58 .value = vv55 .getAccessor().get((inIndex));
                        }
                    }
                    //---- start of eval portion of equal function. ----//
                    NullableBitHolder out61 = new NullableBitHolder();
                    {
                        if (out58 .isSet == 0) {
                            out61 .isSet = 0;
                        } else {
                            final NullableBitHolder out = new NullableBitHolder();
                            NullableBigIntHolder left = out58;
                            IntHolder right = constant60;

    GCompareBigIntInt$EqualsBigIntInt_eval: {
        out.value = left.value == right.value ? 1 : 0;
    }

                            out.isSet = 1;
                            out61 = out;
                            out.isSet = 1;
                        }
                    }
                    //---- end of eval portion of equal function. ----//
                    if ((out61 .isSet == 1)&&(out61 .value == 1)) {
                        out54 = constant63;
                    } else {
                        VarCharHolder out64 = new VarCharHolder();
                        NullableBigIntHolder out68 = new NullableBigIntHolder();
                        {
                            out68 .isSet = vv65 .getAccessor().isSet((inIndex));
                            if (out68 .isSet == 1) {
                                out68 .value = vv65 .getAccessor().get((inIndex));
                            }
                        }
                        //---- start of eval portion of equal function. ----//
                        NullableBitHolder out71 = new NullableBitHolder();
                        {
                            if (out68 .isSet == 0) {
                                out71 .isSet = 0;
                            } else {
                                final NullableBitHolder out = new NullableBitHolder();
                                NullableBigIntHolder left = out68;
                                IntHolder right = constant70;

    GCompareBigIntInt$EqualsBigIntInt_eval: {
        out.value = left.value == right.value ? 1 : 0;
    }

                                out.isSet = 1;
                                out71 = out;
                                out.isSet = 1;
                            }
                        }
                        //---- end of eval portion of equal function. ----//
                        if ((out71 .isSet == 1)&&(out71 .value == 1)) {
                            out64 = constant73;
                        } else {
                            VarCharHolder out74 = new VarCharHolder();
                            NullableBigIntHolder out78 = new NullableBigIntHolder();
                            {
                                out78 .isSet = vv75 .getAccessor().isSet((inIndex));
                                if (out78 .isSet == 1) {
                                    out78 .value = vv75 .getAccessor().get((inIndex));
                                }
                            }
                            //---- start of eval portion of equal function. ----//
                            NullableBitHolder out81 = new NullableBitHolder();
                            {
                                if (out78 .isSet == 0) {
                                    out81 .isSet = 0;
                                } else {
                                    final NullableBitHolder out = new NullableBitHolder();
                                    NullableBigIntHolder left = out78;
                                    IntHolder right = constant80;

    GCompareBigIntInt$EqualsBigIntInt_eval: {
        out.value = left.value == right.value ? 1 : 0;
    }

                                    out.isSet = 1;
                                    out81 = out;
                                    out.isSet = 1;
                                }
                            }
                            //---- end of eval portion of equal function. ----//
                            if ((out81 .isSet == 1)&&(out81 .value == 1)) {
                                out74 = constant83;
                            } else {
                                VarCharHolder out84 = new VarCharHolder();
                                NullableBigIntHolder out88 = new NullableBigIntHolder();
                                {
                                    out88 .isSet = vv85 .getAccessor().isSet((inIndex));
                                    if (out88 .isSet == 1) {
                                        out88 .value = vv85 .getAccessor().get((inIndex));
                                    }
                                }
                                //---- start of eval portion of equal function. ----//
                                NullableBitHolder out91 = new NullableBitHolder();
                                {
                                    if (out88 .isSet == 0) {
                                        out91 .isSet = 0;
                                    } else {
                                        final NullableBitHolder out = new NullableBitHolder();
                                        NullableBigIntHolder left = out88;
                                        IntHolder right = constant90;

    GCompareBigIntInt$EqualsBigIntInt_eval: {
        out.value = left.value == right.value ? 1 : 0;
    }

                                        out.isSet = 1;
                                        out91 = out;
                                        out.isSet = 1;
                                    }
                                }
                                //---- end of eval portion of equal function. ----//
                                if ((out91 .isSet == 1)&&(out91 .value == 1)) {
                                    out84 = constant93;
                                } else {
                                    out84 = constant95;
                                }
                                out74 = out84;
                            }
                            out64 = out74;
                        }
                        out54 = out64;
                    }
                    //---- start of eval portion of castDATE function. ----//
                    DateHolder out96 = new DateHolder();
                    {
                        final DateHolder out = new DateHolder();
                        VarCharHolder in = out54;

    CastVarCharToDate_eval: {
        out.value = org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getDate(in.buffer, in.start, in.end);
    }

                        out96 = out;
                    }
                    //---- end of eval portion of castDATE function. ----//
                    //---- start of eval portion of greater_than_or_equal_to function. ----//
                    NullableBitHolder out97 = new NullableBitHolder();
                    {
                        if (out53 .isSet == 0) {
                            out97 .isSet = 0;
                        } else {
                            final NullableBitHolder out = new NullableBitHolder();
                            NullableDateHolder left = out53;
                            DateHolder right = out96;

    GCompareDateFunctions$GreaterThanEDate_eval: {
        out.value = left.value >= right.value ? 1 : 0;
    }

                            out.isSet = 1;
                            out97 = out;
                            out.isSet = 1;
                        }
                    }
                    //---- end of eval portion of greater_than_or_equal_to function. ----//
                    if ((out97 .isSet == 1)&&(out97 .value!= 1)) {
                        out0 .isSet = 1;
                        out0 .value = 0;
                        break AndOP0;
                    }
                    if ((((out7 .isSet*out8 .isSet)*out48 .isSet)*out97 .isSet) == 0) {
                        out0 .isSet = 0;
                    } else {
                        out0 .isSet = 1;
                        out0 .value = 1;
                    }
                }
                return (out0 .value == 1);
            }
        }

        public void __DRILL_INIT__()
            throws SchemaChangeException
        {
        }

    }
