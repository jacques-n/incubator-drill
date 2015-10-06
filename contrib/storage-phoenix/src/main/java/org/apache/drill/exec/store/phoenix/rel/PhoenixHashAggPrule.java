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
package org.apache.drill.exec.store.phoenix.rel;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.physical.HashAggPrel;
import org.apache.drill.exec.planner.physical.Prel;
import org.apache.drill.exec.store.phoenix.PhoenixIntermediatePrel;
import org.apache.phoenix.calcite.rel.PhoenixRel;

public class PhoenixHashAggPrule extends RelOptRule {
  
  public static final PhoenixHashAggPrule INSTANCE = new PhoenixHashAggPrule();

  public PhoenixHashAggPrule() {
    super(
        operand(HashAggPrel.class, 
                operand(PhoenixIntermediatePrel.class, any())));
  }

  public PhoenixHashAggPrule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    HashAggPrel hashAgg = (HashAggPrel) call.rel(0);
    PhoenixIntermediatePrel phoenixPrel = (PhoenixIntermediatePrel) call.rel(1);
    RelNode child = phoenixPrel.getInput();
    if (child.getConvention() != PhoenixRel.SERVER_CONVENTION)
      return;
    
    PhoenixServerAggregate newAgg = PhoenixServerAggregate.create(
        child, 
        hashAgg.indicator, 
        hashAgg.getGroupSet(), 
        hashAgg.groupSets, 
        hashAgg.getAggCallList());
    RelNode newRel = convert(newAgg, newAgg.getTraitSet().replace(DrillRel.DRILL_LOGICAL));
    newRel = convert(newRel, newRel.getTraitSet().replace(Prel.DRILL_PHYSICAL));
    call.transformTo(newRel);
  }

}
