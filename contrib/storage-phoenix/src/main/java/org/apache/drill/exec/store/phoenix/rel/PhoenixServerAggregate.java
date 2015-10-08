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

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.phoenix.calcite.rel.PhoenixAbstractAggregate;
import org.apache.phoenix.calcite.rel.PhoenixRel;
import org.apache.phoenix.compile.GroupByCompiler.GroupBy;
import org.apache.phoenix.compile.OrderByCompiler.OrderBy;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.compile.RowProjector;
import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.execute.AggregatePlan;
import org.apache.phoenix.execute.ScanPlan;

public class PhoenixServerAggregate extends PhoenixAbstractAggregate implements PhoenixDrillRel {

  public static PhoenixServerAggregate create(RelNode input, boolean indicator, 
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, 
      List<AggregateCall> aggCalls) {
    RelOptCluster cluster = input.getCluster();
    RelTraitSet traits = cluster.traitSetOf(PhoenixDrillRel.SERVER_INTERMEDIATE_CONVENTION);
    return new PhoenixServerAggregate(cluster, traits, input, indicator, 
        groupSet, groupSets, aggCalls);
  }

  private PhoenixServerAggregate(RelOptCluster cluster, RelTraitSet traits,
      RelNode child, boolean indicator, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
  }

  @Override
  public PhoenixServerAggregate copy(RelTraitSet traits, RelNode input, boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggregateCalls) {
    return create(input, indicator, groupSet, groupSets, aggregateCalls);
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
    implementor.pushContext(new ImplementorContext(true, false, getColumnRefList()));
    QueryPlan plan = implementor.visitInput(0, (PhoenixRel) getInput());
    implementor.popContext();

    assert plan instanceof ScanPlan
      && plan.getLimit() == null;

    ScanPlan scanPlan = (ScanPlan) plan;
    StatementContext context = scanPlan.getContext();        
    GroupBy groupBy = super.getGroupBy(implementor);       
    super.serializeAggregators(implementor, context, groupBy.isEmpty());

    QueryPlan aggPlan = new AggregatePlan(context, scanPlan.getStatement(), scanPlan.getTableRef(), RowProjector.EMPTY_PROJECTOR, null, OrderBy.EMPTY_ORDER_BY, null, groupBy, null, scanPlan.getDynamicFilter());
    return PhoenixAbstractAggregate.wrapWithProject(
        implementor, aggPlan, groupBy.getKeyExpressions(), 
        Arrays.asList(context.getAggregationManager().getAggregators().getFunctions()));
  }

}
