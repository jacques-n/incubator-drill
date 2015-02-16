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
package org.apache.drill.exec.planner.logical.partition;

import java.util.BitSet;
import java.util.List;

import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexCorrelVariable;
import org.eigenbase.rex.RexDynamicParam;
import org.eigenbase.rex.RexFieldAccess;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexOver;
import org.eigenbase.rex.RexRangeRef;
import org.eigenbase.rex.RexVisitorImpl;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlRowOperator;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.Stacks;
import org.eigenbase.util.Util;

import com.google.common.collect.Lists;


public class FindPartitionConditions extends RexVisitorImpl<Void> {
  /** Whether an expression is constant, and if so, whether it can be
   * reduced to a simpler constant. */
  enum Constancy {
    NO_PUSH, PUSH
  }

  private final BitSet dirs;

  private final List<Constancy> stack =  Lists.newArrayList();
  private final List<RexNode> partitionSubTrees = Lists.newArrayList();
  private final List<SqlOperator> parentCallTypeStack = Lists.newArrayList();

  public FindPartitionConditions(BitSet dirs) {
    // go deep
    super(true);
    this.dirs = dirs;
  }

  public void analyze(RexNode exp) {
    assert stack.isEmpty();

    exp.accept(this);

    // Deal with top of stack
    assert stack.size() == 1;
    assert parentCallTypeStack.isEmpty();
    Constancy rootConstancy = stack.get(0);
    if (rootConstancy == Constancy.PUSH) {
      // The entire subtree was constant, so add it to the result.
      addResult(exp);
    }
    stack.clear();
  }

  public RexNode getSubTree(RexBuilder builder){
    if(partitionSubTrees.isEmpty()) {
      return null;
    }

    if(partitionSubTrees.size() == 1){
      return partitionSubTrees.get(0);
    }

    return builder.makeCall(SqlStdOperatorTable.AND, partitionSubTrees);
  }


  private Void pushVariable() {
    stack.add(Constancy.NO_PUSH);
    return null;
  }

  private void addResult(RexNode exp) {
    partitionSubTrees.add(exp);
  }


  public Void visitInputRef(RexInputRef inputRef) {
    if(dirs.get(inputRef.getIndex())){
      stack.add(Constancy.PUSH);
    }else{
      stack.add(Constancy.NO_PUSH);
    }
    return null;
  }

  public Void visitLiteral(RexLiteral literal) {
    stack.add(Constancy.PUSH);
    return null;
  }

  public Void visitOver(RexOver over) {
    // assume non-constant (running SUM(1) looks constant but isn't)
    analyzeCall(over, Constancy.NO_PUSH);
    return null;
  }

  public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
    return pushVariable();
  }

  public Void visitCall(RexCall call) {
    // assume REDUCIBLE_CONSTANT until proven otherwise
    analyzeCall(call, Constancy.PUSH);
    return null;
  }

  private void analyzeCall(RexCall call, Constancy callConstancy) {
    Stacks.push(parentCallTypeStack, call.getOperator());

    // visit operands, pushing their states onto stack
    super.visitCall(call);

    // look for NO_PUSH operands
    int operandCount = call.getOperands().size();
    List<Constancy> operandStack = Util.last(stack, operandCount);
    for (Constancy operandConstancy : operandStack) {
      if (operandConstancy == Constancy.NO_PUSH) {
        callConstancy = Constancy.NO_PUSH;
      }
    }

    // Even if all operands are PUSH, the call itself may
    // be non-deterministic.
    if (!call.getOperator().isDeterministic()) {
      callConstancy = Constancy.NO_PUSH;
    } else if (call.getOperator().isDynamicFunction()) {
      // We can reduce the call to a constant, but we can't
      // cache the plan if the function is dynamic.
      // For now, treat it same as non-deterministic.
      callConstancy = Constancy.NO_PUSH;
    }

    // Row operator itself can't be reduced to a PUSH
    if ((callConstancy == Constancy.PUSH)
        && (call.getOperator() instanceof SqlRowOperator)) {
      callConstancy = Constancy.NO_PUSH;
    }


    if (callConstancy == Constancy.NO_PUSH && call.getKind() == SqlKind.AND) {
      // one or more children is is not a constant.  If this is an AND, add all the ones that are constant.  Otherwise, this tree cannot be pushed.
      for (int iOperand = 0; iOperand < operandCount; ++iOperand) {
        Constancy constancy = operandStack.get(iOperand);
        if (constancy == Constancy.PUSH) {
          addResult(call.getOperands().get(iOperand));
        }
      }
    }


    // pop operands off of the stack
    operandStack.clear();

    // pop this parent call operator off the stack
    Stacks.pop(parentCallTypeStack, call.getOperator());

    // push constancy result for this call onto stack
    stack.add(callConstancy);
  }

  public Void visitDynamicParam(RexDynamicParam dynamicParam) {
    return pushVariable();
  }

  public Void visitRangeRef(RexRangeRef rangeRef) {
    return pushVariable();
  }

  public Void visitFieldAccess(RexFieldAccess fieldAccess) {
    return pushVariable();
  }


}
