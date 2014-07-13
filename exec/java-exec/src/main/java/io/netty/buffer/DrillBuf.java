package io.netty.buffer;

import org.jboss.netty.util.DebugUtil;

import io.netty.util.internal.PlatformDependent;

public class DrillBuf extends SlicedByteBuf {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillBuf.class);

  private final long memoryAddress;
  private final ByteBuf underlying;

  public DrillBuf(SlicedByteBuf buffer) {
    super(buffer, index, length);
    this.memoryAddress = buffer.memoryAddress() + index;
    this.

  }

  private long addr(int index) {
    return memoryAddress + index;
  }

  private void check(int index, int size) {
    if (DebugUtil.isDebugEnabled()) {
      wrapped.checkIndex(index, size);
    }
  }

  @Override
  public long getLong(int index) {
      wrapped.checkIndex(index, 8);
      long v = PlatformDependent.getLong(addr(index));
      return v;
  }

  @Override
  public float getFloat(int index) {
      return Float.intBitsToFloat(getInt(index));
  }

  @Override
  public double getDouble(int index) {
      return Double.longBitsToDouble(getLong(index));
  }

  @Override
  public char getChar(int index) {
      return (char) getShort(index);
  }

  @Override
  public long getUnsignedInt(int index) {
      return getInt(index) & 0xFFFFFFFFL;
  }

  @Override
  public int getInt(int index) {
      wrapped.checkIndex(index, 4);
      int v = PlatformDependent.getInt(addr(index));
      return v;
  }

  @Override
  public int getUnsignedShort(int index) {
      return getShort(index) & 0xFFFF;
  }

  @Override
  public short getShort(int index) {
      wrapped.checkIndex(index, 2);
      short v = PlatformDependent.getShort(addr(index));
      return v;
  }

  @Override
  public ByteBuf setShort(int index, int value) {
      wrapped.checkIndex(index, 2);
      _setShort(index, value);
      return this;
  }

  @Override
  public ByteBuf setInt(int index, int value) {
      wrapped.checkIndex(index, 4);
      _setInt(index, value);
      return this;
  }

  @Override
  public ByteBuf setLong(int index, long value) {
      wrapped.checkIndex(index, 8);
      _setLong(index, value);
      return this;
  }

  @Override
  public ByteBuf setChar(int index, int value) {
      setShort(index, value);
      return this;
  }

  @Override
  public ByteBuf setFloat(int index, float value) {
      setInt(index, Float.floatToRawIntBits(value));
      return this;
  }

  @Override
  public ByteBuf setDouble(int index, double value) {
      setLong(index, Double.doubleToRawLongBits(value));
      return this;
  }

  @Override
  public ByteBuf writeShort(int value) {
      wrapped.ensureWritable(2);
      _setShort(wrapped.writerIndex, value);
      wrapped.writerIndex += 2;
      return this;
  }

  @Override
  public ByteBuf writeInt(int value) {
      wrapped.ensureWritable(4);
      _setInt(wrapped.writerIndex, value);
      wrapped.writerIndex += 4;
      return this;
  }

  @Override
  public ByteBuf writeLong(long value) {
      wrapped.ensureWritable(8);
      _setLong(wrapped.writerIndex, value);
      wrapped.writerIndex += 8;
      return this;
  }

  @Override
  public ByteBuf writeChar(int value) {
      writeShort(value);
      return this;
  }

  @Override
  public ByteBuf writeFloat(float value) {
      writeInt(Float.floatToRawIntBits(value));
      return this;
  }

  @Override
  public ByteBuf writeDouble(double value) {
      writeLong(Double.doubleToRawLongBits(value));
      return this;
  }

  private void _setShort(int index, int value) {
      PlatformDependent.putShort(addr(index), (short) value);
  }

  private void _setInt(int index, int value) {
      PlatformDependent.putInt(addr(index), value);
  }

  private void _setLong(int index, long value) {
      PlatformDependent.putLong(addr(index), value);
  }
}
