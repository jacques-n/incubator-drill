package org.apache.drill.exec.planner.sql.handlers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import net.hydromatic.optiq.tools.Planner;
import net.hydromatic.optiq.tools.RelConversionException;
import net.hydromatic.optiq.tools.RuleSet;
import net.hydromatic.optiq.tools.ValidationException;

import org.apache.drill.common.logical.PlanProperties;
import org.apache.drill.common.logical.PlanProperties.Generator.ResultMode;
import org.apache.drill.common.logical.PlanProperties.PlanPropertiesBuilder;
import org.apache.drill.common.logical.PlanProperties.PlanType;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.AbstractPhysicalVisitor;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.logical.DrillRel;
import org.apache.drill.exec.planner.logical.DrillRuleSets;
import org.apache.drill.exec.planner.logical.DrillScreenRel;
import org.apache.drill.exec.planner.logical.DrillStoreRel;
import org.apache.drill.exec.planner.physical.DrillDistributionTrait;
import org.apache.drill.exec.planner.physical.PhysicalPlanCreator;
import org.apache.drill.exec.planner.physical.PlanningSettings;
import org.apache.drill.exec.planner.physical.Prel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.sql.SqlNode;

import com.google.common.base.Preconditions;
import com.google.hive12.common.collect.Lists;

public class DefaultSqlHandler implements SqlHandler{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DefaultSqlHandler.class);

  private final static RuleSet[] RULES = new RuleSet[]{DrillRuleSets.DRILL_BASIC_RULES, DrillRuleSets.DRILL_PHYSICAL_MEM};
  private final static int LOGICAL_RULES = 0;
  private final static int PHYSICAL_MEM_RULES = 1;

  protected final Planner planner;
  protected final QueryContext context;


  public DefaultSqlHandler(Planner planner, QueryContext context) {
    super();
    this.planner = planner;
    this.context = context;
  }

  @Override
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException {
    SqlNode validated = validateNode(sqlNode);
    RelNode rel = convertToRel(validated);
    DrillRel drel = convertToDrel(rel);
    Prel prel = convertToPrel(drel);
    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    return plan;
  }

  protected SqlNode validateNode(SqlNode sqlNode) throws ValidationException, RelConversionException{
    return planner.validate(sqlNode);
  }

  protected RelNode convertToRel(SqlNode node) throws RelConversionException{
    return planner.convert(node);
  }

  protected DrillRel convertToDrel(RelNode relNode) throws RelConversionException{
    RelNode convertedRelNode = planner.transform(LOGICAL_RULES, relNode.getTraitSet().plus(DrillRel.DRILL_LOGICAL), relNode);
    if(convertedRelNode instanceof DrillStoreRel){
      throw new UnsupportedOperationException();
    }else{
      return new DrillScreenRel(convertedRelNode.getCluster(), convertedRelNode.getTraitSet(), convertedRelNode);
    }
  }

  protected Prel convertToPrel(RelNode drel) throws RelConversionException{
    Preconditions.checkArgument(drel.getConvention() == DrillRel.DRILL_LOGICAL);
    RelTraitSet traits = drel.getTraitSet().plus(Prel.DRILL_PHYSICAL).plus(DrillDistributionTrait.SINGLETON);
    Prel phyRelNode = (Prel) planner.transform(PHYSICAL_MEM_RULES, traits, drel);
    return phyRelNode;
  }

  protected PhysicalOperator convertToPop(Prel prel) throws IOException{

    boolean singleMode = !context.getSession().isEnableExchanges();

    if(singleMode) PlanningSettings.get().setSingleMode(true);
    PhysicalPlanCreator creator = new PhysicalPlanCreator(context);
    PhysicalOperator op =  prel.getPhysicalOperator(creator);

    if(singleMode) PlanningSettings.get().setSingleMode(false);
    return op;
  }

  protected PhysicalPlan convertToPlan(PhysicalOperator op){
    PlanPropertiesBuilder propsBuilder = PlanProperties.builder();
    propsBuilder.type(PlanType.APACHE_DRILL_PHYSICAL);
    propsBuilder.version(1);
    propsBuilder.resultMode(ResultMode.EXEC);
    propsBuilder.generator(this.getClass().getSimpleName(), "");
    return new PhysicalPlan(propsBuilder.build(), getPops(op));
  }


  public static List<PhysicalOperator> getPops(PhysicalOperator root){
    List<PhysicalOperator> ops = Lists.newArrayList();
    PopCollector c = new PopCollector();
    root.accept(c, ops);
    return ops;
  }

  private static class PopCollector extends AbstractPhysicalVisitor<Void, Collection<PhysicalOperator>, RuntimeException>{

    @Override
    public Void visitOp(PhysicalOperator op, Collection<PhysicalOperator> collection) throws RuntimeException {
      collection.add(op);
      for(PhysicalOperator o : op){
        o.accept(this, collection);
      }
      return null;
    }

  }

  public static <T> T unwrap(Object o, Class<T> clazz) throws RelConversionException{
    if(clazz.isAssignableFrom(o.getClass())){
      return (T) o;
    }else{
      throw new RelConversionException(String.format("Failure trying to treat %s as type %s.", o.getClass().getSimpleName(), clazz.getSimpleName()));
    }
  }
}
