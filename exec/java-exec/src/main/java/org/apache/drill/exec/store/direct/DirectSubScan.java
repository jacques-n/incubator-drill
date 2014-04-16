package org.apache.drill.exec.store.direct;

import org.apache.drill.exec.physical.base.AbstractSubScan;
import org.apache.drill.exec.store.RecordReader;

public class DirectSubScan extends AbstractSubScan{
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DirectSubScan.class);

  private final RecordReader reader;

  public DirectSubScan(RecordReader reader) {
    super();
    this.reader = reader;
  }

  public RecordReader getReader() {
    return reader;
  }

}
