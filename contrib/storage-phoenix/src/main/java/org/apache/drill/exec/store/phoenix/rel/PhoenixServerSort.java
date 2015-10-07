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

import java.sql.SQLException;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.phoenix.calcite.rel.PhoenixAbstractSort;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.execute.ScanPlan;

public class PhoenixServerSort extends PhoenixAbstractSort implements PhoenixDrillRel {
  
  public static PhoenixServerSort create(RelNode input, RelCollation collation) {
      RelOptCluster cluster = input.getCluster();
      collation = RelCollationTraitDef.INSTANCE.canonize(collation);
      RelTraitSet traits =
          input.getTraitSet().replace(PhoenixDrillRel.SERVER_INTERMEDIATE_CONVENTION).replace(collation);
      return new PhoenixServerSort(cluster, traits, input, collation);
  }

  private PhoenixServerSort(RelOptCluster cluster, RelTraitSet traits,
      RelNode child, RelCollation collation) {
    super(cluster, traits, child, collation);
  }

  @Override
  public Sort copy(RelTraitSet traitSet, RelNode newInput,
      RelCollation newCollation, RexNode offset, RexNode fetch) {
    return create(newInput, newCollation);
  }
  
  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
      if (getInput().getConvention() != PhoenixRel.SERVER_CONVENTION)
          return planner.getCostFactory().makeInfiniteCost();
      
      return super.computeSelfCost(planner)
              .multiplyBy(SERVER_FACTOR)
              .multiplyBy(PHOENIX_FACTOR);
  }

  @Override
  public QueryPlan implement(Implementor implementor) {
    if (this.offset != null)
      throw new UnsupportedOperationException();

    final QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
    assert plan instanceof ScanPlan && plan.getLimit() == null;

    final OrderBy orderBy = super.getOrderBy(implementor, null);
    final QueryPlan newPlan;
    try {
      newPlan = ScanPlan.create((ScanPlan) plan, orderBy);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    return newPlan;
  }

}
