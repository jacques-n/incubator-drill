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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.Charset;

import org.apache.drill.exec.memory.Accountor;

public class AccountingByteBuf extends AbstractByteBuf {
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

  public boolean transferAccounting(Accountor target){
    if(rootBuffer){
      boolean outcome = acct.transferTo(target, this, length);
      acct = target;
      return outcome;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public synchronized boolean release() {
    if(b.release() && rootBuffer){
      acct.release(this, length);
      return true;
    }
    return false;
  }

  @Override
  public synchronized boolean release(int decrement) {
    if(b.release(decrement) && rootBuffer){
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
    if(rootBuffer){
      if(newCapacity == length){
        return this;
      }else if(newCapacity < length){
        b.capacity(newCapacity);
        int diff = length - b.capacity();
        acct.releasePartial(this, diff);
        this.length = length - diff;
        return this;
      }else{
        throw new UnsupportedOperationException("Accounting byte buf doesn't support increasing allocations.");
      }
    }else{
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
//    if(endianness != ByteOrder.LITTLE_ENDIAN) throw new UnsupportedOperationException("Drill buffers only support little endian.");
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
  public ByteBuf setZero(int index, int length) {
    b.setZero(index, length);
    return this;
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
  public ByteBuf slice(int index, int length) {
      return new AccountingByteBuf(null, this, index, length);
  }

  @Override
  public ByteBuf duplicate() {
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
    return b.nioBuffer(index, length);
  }

  @Override
  public ByteBuffer internalNioBuffer(int index, int length) {
    return b.internalNioBuffer(index, length);
  }

  @Override
  public ByteBuffer[] nioBuffers() {
    return b.nioBuffers();
  }

  @Override
  public ByteBuffer[] nioBuffers(int index, int length) {
    return b.nioBuffers(index, length);
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
    return b.hasMemoryAddress();
  }

  @Override
  public long memoryAddress() {
    return b.memoryAddress();
  }

  @Override
  public String toString(Charset charset) {
    return b.toString(charset);
  }

  @Override
  public String toString(int index, int length, Charset charset) {
    return b.toString(index, length, charset);
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
    return "AccountingByteBuf [Inner buffer=" + b + ", size=" + length + "]";
  }

  @Override
  public int compareTo(ByteBuf buffer) {
    return b.compareTo(buffer);
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


}
