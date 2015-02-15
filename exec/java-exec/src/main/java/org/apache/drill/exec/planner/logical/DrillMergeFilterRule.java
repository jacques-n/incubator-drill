package org.apache.drill.exec.planner.logical;

import org.eigenbase.rel.FilterRel;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexProgram;
import org.eigenbase.rex.RexProgramBuilder;
import org.eigenbase.rex.RexUtil;

/**
 * MergeFilterRule implements the rule for combining two {@link FilterRel}s
 */
public class DrillMergeFilterRule extends RelOptRule {
  public static final DrillMergeFilterRule INSTANCE = new DrillMergeFilterRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a MergeFilterRule.
   */
  private DrillMergeFilterRule() {
    super(
        operand(
            FilterRel.class,
            operand(FilterRel.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    FilterRel topFilter = call.rel(0);
    FilterRel bottomFilter = call.rel(1);

    // use RexPrograms to merge the two FilterRels into a single program
    // so we can convert the two FilterRel conditions to directly
    // reference the bottom FilterRel's child
    RexBuilder rexBuilder = topFilter.getCluster().getRexBuilder();
    RexProgram bottomProgram = createProgram(bottomFilter);
    RexProgram topProgram = createProgram(topFilter);

    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder);

    RexNode newCondition =
        mergedProgram.expandLocalRef(
            mergedProgram.getCondition());

    if(!RexUtil.isFlat(newCondition)){
      RexCall newCall = (RexCall) newCondition;
      newCondition = rexBuilder.makeFlatCall( newCall.getOperator(), newCall.getOperands());
    }

    FilterRel newFilterRel =
        new FilterRel(
            topFilter.getCluster(),
            bottomFilter.getChild(),
            newCondition);

    call.transformTo(newFilterRel);
  }

  /**
   * Creates a RexProgram corresponding to a FilterRel
   *
   * @param filterRel the FilterRel
   * @return created RexProgram
   */
  private RexProgram createProgram(FilterRel filterRel) {
    RexProgramBuilder programBuilder =
        new RexProgramBuilder(
            filterRel.getRowType(),
            filterRel.getCluster().getRexBuilder());
    programBuilder.addIdentity();
    programBuilder.addCondition(filterRel.getCondition());
    return programBuilder.getProgram();
  }
}
