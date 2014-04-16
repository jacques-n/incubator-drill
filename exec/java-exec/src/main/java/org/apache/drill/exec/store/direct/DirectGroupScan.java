package org.apache.drill.exec.store.direct;

import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.OperatorCost;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.Size;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.RecordReader;

public class DirectGroupScan extends AbstractGroupScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectGroupScan.class);

  private final RecordReader reader;

  public DirectGroupScan(RecordReader reader) {
    super();
    this.reader = reader;
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> endpoints) throws PhysicalOperatorSetupException {
    assert endpoints.size() == 1;
  }

  @Override
  public SubScan getSpecificScan(int minorFragmentId) throws ExecutionSetupException {
    assert minorFragmentId == 0;
    return new DirectSubScan(reader);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public OperatorCost getCost() {
    return new OperatorCost(1,1,1,1);
  }

  @Override
  public Size getSize() {
    return new Size(1,1);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    assert children == null || children.isEmpty();
    return this;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }

}
