package org.apache.drill.exec.store.direct;

import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;

public class DirectBatchCreator implements BatchCreator<DirectSubScan>{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectBatchCreator.class);

  @Override
  public RecordBatch getBatch(FragmentContext context, DirectSubScan config, List<RecordBatch> children)
      throws ExecutionSetupException {
    return new ScanBatch(context, Collections.singleton(config.getReader()).iterator());
  }
}
