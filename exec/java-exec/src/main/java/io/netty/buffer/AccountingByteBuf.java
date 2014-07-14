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
package io.netty.buffer;

import io.netty.util.internal.PlatformDependent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

import org.apache.drill.exec.memory.Accountor;
import org.jboss.netty.util.DebugUtil;

public final class AccountingByteBuf extends AbstractByteBuf {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccountingByteBuf.class);

  private final ByteBuf b;
  private volatile Accountor acct;
  private final long addr;
  private final int offset;


  private volatile int length;

  private boolean rootBuffer;

  public AccountingByteBuf(Accountor a, UnsafeDirectLittleEndian b) {
    super(b.maxCapacity());
    this.b = b;
    this.addr = b.memoryAddress();
    this.acct = a;
    this.length = b.capacity();
    this.offset = 0;
    this.rootBuffer = true;
  }

  private AccountingByteBuf(Accountor a, AccountingByteBuf buffer, int index, int length) {
    super(length);
    if (index < 0 || index > buffer.capacity() - length) {
      throw new IndexOutOfBoundsException(buffer.toString() + ".slice(" + index + ", " + length + ')');
    }

    this.length = length;
    writerIndex(length);

    this.b = buffer;
    this.addr = buffer.memoryAddress() + index;
    this.offset = index;
    this.acct = null;
    this.length = length;
    this.rootBuffer = false;
  }

  @Override
  public int refCnt() {
    return b.refCnt();
  }

  private long addr(int index) {
    return addr + index;
  }

  private void chk(int index, int width) {
    if (DebugUtil.isDebugEnabled()) {
      checkIndex(index, width);
    }
  }

  private void chk(int index) {
    if (DebugUtil.isDebugEnabled()) {
      checkIndex(index);
    }
  }

  private void ensure(int width) {
    if (DebugUtil.isDebugEnabled()) {
      ensureWritable(width);
    }
  }

  public boolean transferAccounting(Accountor target) {
    if (rootBuffer) {
      boolean outcome = acct.transferTo(target, this, length);
      acct = target;
      return outcome;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public synchronized boolean release() {
    if (b.release() && rootBuffer) {
      acct.release(this, length);
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean release(int decrement) {
    if (b.release(decrement) && rootBuffer) {
      acct.release(this, length);
      return true;
    }
    return false;
  }

  @Override
  public int capacity() {
    return length;
  }

  @Override
  public synchronized ByteBuf capacity(int newCapacity) {
    if (rootBuffer) {
      if (newCapacity == length) {
        return this;
      } else if (newCapacity < length) {
        b.capacity(newCapacity);
        int diff = length - b.capacity();
        acct.releasePartial(this, diff);
        this.length = length - diff;
        return this;
      } else {
        throw new UnsupportedOperationException("Accounting byte buf doesn't support increasing allocations.");
      }
    } else {
      throw new UnsupportedOperationException("Non root bufs doen't support changing allocations.");
    }
  }

  @Override
  public int maxCapacity() {
    return length;
  }

  @Override
  public ByteBufAllocator alloc() {
    return b.alloc();
  }

  @Override
  public ByteOrder order() {
    return ByteOrder.LITTLE_ENDIAN;
  }

  @Override
  public ByteBuf order(ByteOrder endianness) {
    // if(endianness != ByteOrder.LITTLE_ENDIAN) throw new
    // UnsupportedOperationException("Drill buffers only support little endian.");
    return this;
  }

  @Override
  public ByteBuf unwrap() {
    return this;
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public ByteBuf readBytes(int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf readSlice(int length) {
    ByteBuf slice = slice(readerIndex(), length);
    readerIndex(readerIndex() + length);
    return slice;
  }

  @Override
  public ByteBuf copy() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf copy(int index, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteBuf slice() {
    return slice(b.readerIndex(), readableBytes());
  }

  @Override
  public AccountingByteBuf slice(int index, int length) {
    return new AccountingByteBuf(null, this, index, length);
  }

  @Override
  public AccountingByteBuf duplicate() {
    return new AccountingByteBuf(null, this, 0, length);
  }

  @Override
  public int nioBufferCount() {
    return b.nioBufferCount();
  }

  @Override
  public ByteBuffer nioBuffer() {
    return b.nioBuffer();
  }

  @Override
  public ByteBuffer nioBuffer(int index, int length) {
    return b.nioBuffer(offset + index, length);
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return b.internalNioBuffer(offset + index, length);
  }

  @Override
  public ByteBuffer[] nioBuffers() {
    return b.nioBuffers();
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return b.nioBuffers(offset + index, length);
  }

  @Override
  public boolean hasArray() {
    return b.hasArray();
  }

  @Override
  public byte[] array() {
    return b.array();
  }

  @Override
  public int arrayOffset() {
    return b.arrayOffset();
  }

  @Override
  public boolean hasMemoryAddress() {
    return true;
  }

  @Override
  public long memoryAddress() {
    return this.addr;
  }

  @Override
  public String toString(Charset charset) {
    return b.toString(charset);
  }

  @Override
  public String toString(int index, int length, Charset charset) {
    return b.toString(offset + index, length, charset);
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public boolean equals(Object obj) {
    // identity equals only.
    return this == obj;
  }

  @Override
  public String toString() {
    return "DrillBuf [Inner buffer=" + b + ", size=" + length + "]";
  }

  @Override
  public ByteBuf retain(int increment) {
    b.retain(increment);
    return this;
  }

  @Override
  public ByteBuf retain() {
    b.retain();
    return this;
  }

  @Override
  public long getLong(int index) {
    chk(index, 8);
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
    chk(index, 4);
    int v = PlatformDependent.getInt(addr(index));
    return v;
  }

  @Override
  public int getUnsignedShort(int index) {
    return getShort(index) & 0xFFFF;
  }

  @Override
  public short getShort(int index) {
    chk(index, 2);
    short v = PlatformDependent.getShort(addr(index));
    return v;
  }

  @Override
  public ByteBuf setShort(int index, int value) {
    chk(index, 2);
    PlatformDependent.putShort(addr(index), (short) value);
    return this;
  }

  @Override
  public ByteBuf setInt(int index, int value) {
    chk(index, 4);
    PlatformDependent.putInt(addr(index), value);
    return this;
  }

  @Override
  public ByteBuf setLong(int index, long value) {
    chk(index, 8);
    PlatformDependent.putLong(index, value);
    return this;
  }

  @Override
  public ByteBuf setChar(int index, int value) {
    chk(index, 2);
    PlatformDependent.putShort(addr(index), (short) value);
    return this;
  }

  @Override
  public ByteBuf setFloat(int index, float value) {
    chk(index, 4);
    PlatformDependent.putInt(addr(index), Float.floatToRawIntBits(value));
    return this;
  }

  @Override
  public ByteBuf setDouble(int index, double value) {
    chk(index, 8);
    PlatformDependent.putLong(index, Double.doubleToRawLongBits(value));
    return this;
  }

  @Override
  public ByteBuf writeShort(int value) {
    ensure(2);
    PlatformDependent.putShort(addr(writerIndex), (short) value);
    writerIndex += 2;
    return this;
  }

  @Override
  public ByteBuf writeInt(int value) {
    ensure(4);
    PlatformDependent.putInt(addr(writerIndex), value);
    writerIndex += 4;
    return this;
  }

  @Override
  public ByteBuf writeLong(long value) {
    ensure(8);
    PlatformDependent.putLong(addr(writerIndex), value);
    writerIndex += 8;
    return this;
  }

  @Override
  public ByteBuf writeChar(int value) {
    ensure(2);
    PlatformDependent.putShort(addr(writerIndex), (short) value);
    writerIndex += 2;
    return this;
  }

  @Override
  public ByteBuf writeFloat(float value) {
    ensure(4);
    PlatformDependent.putInt(addr(writerIndex), Float.floatToRawIntBits(value));
    writerIndex += 4;
    return this;
  }

  @Override
  public ByteBuf writeDouble(double value) {
    ensure(8);
    PlatformDependent.putLong(addr(writerIndex), Double.doubleToRawLongBits(value));
    writerIndex += 8;
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, byte[] dst, int dstIndex, int length) {
    chk(index, length);
    if (dst == null) {
      throw new NullPointerException("null destination");
    }
    if (dstIndex < 0 || dstIndex > dst.length - length) {
      throw new IndexOutOfBoundsException("dstIndex: " + dstIndex);
    }
    if (length != 0) {
      PlatformDependent.copyMemory(addr(index), dst, dstIndex, length);
    }
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, ByteBuffer dst) {
    getBytes(index, dst, false);
    return this;
  }


  @Override
  public ByteBuf readBytes(ByteBuffer dst) {
    int length = dst.remaining();
    checkReadableBytes(length);
    getBytes(readerIndex, dst, true);
    readerIndex += length;
    return this;
  }

  @Override
  public ByteBuf getBytes(int index, OutputStream out, int length) throws IOException {
    chk(index, length);
    if (length != 0) {
      byte[] tmp = new byte[length];
      PlatformDependent.copyMemory(addr(index), tmp, 0, length);
      out.write(tmp);
    }
    return this;
  }

  @Override
  public int getBytes(int index, GatheringByteChannel out, int length) throws IOException {
    return getBytes(index, out, length, false);
  }

  private int getBytes(int index, GatheringByteChannel out, int length, boolean internal) throws IOException {
    checkIndex(index, length);
    if (length == 0) {
      return 0;
    }

    ByteBuffer tmpBuf;
    if (internal) {
      tmpBuf = internalNioBuffer();
    } else {
      tmpBuf = memory.duplicate();
    }
    index = idx(index);
    tmpBuf.clear().position(index).limit(index + length);
    return out.write(tmpBuf);
  }

  private void getBytes(int index, ByteBuffer dst, boolean internal) {
    checkIndex(index);
    int bytesToCopy = Math.min(capacity() - index, dst.remaining());
    ByteBuffer tmpBuf;
    if (internal) {
      tmpBuf = internalNioBuffer();
    } else {
      tmpBuf = memory.duplicate();
    }
    index = idx(index);
    tmpBuf.clear().position(index).limit(index + bytesToCopy);
    dst.put(tmpBuf);
  }

  private final ByteBuffer internalNioBuffer() {
    ByteBuffer tmpNioBuf = this.tmpNioBuf;
    if (tmpNioBuf == null) {
      this.tmpNioBuf = tmpNioBuf = newInternalNioBuffer(memory);
    }
    return tmpNioBuf;
  }

  @Override
  public int readBytes(GatheringByteChannel out, int length) throws IOException {
    checkReadableBytes(length);
    int readBytes = getBytes(readerIndex, out, length, true);
    readerIndex += readBytes;
    return readBytes;
  }

}
