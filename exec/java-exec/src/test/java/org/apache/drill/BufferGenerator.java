package org.apache.drill;

import io.netty.buffer.DrillBuf;

public interface BufferGenerator {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BufferGenerator.class);

  public DrillBuf getBuf(int size);
  public DrillBuf resizeBuf(int size, DrillBuf oldBuf);
}
