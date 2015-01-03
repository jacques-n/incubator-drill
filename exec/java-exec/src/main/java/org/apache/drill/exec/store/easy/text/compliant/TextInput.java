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
package org.apache.drill.exec.store.easy.text.compliant;

/*******************************************************************************
 * Copyright 2014 uniVocity Software Pty Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

import io.netty.buffer.DrillBuf;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.fs.AbstractFileSystem;
import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;

import com.carrotsearch.hppc.ByteArrayList;
import com.google.common.base.Charsets;
import com.univocity.parsers.common.Format;

public final class TextInput {

  private static final byte NULL_BYTE = (byte) '\0';
//  private final byte lineSeparator1;
//  private final byte lineSeparator2;
//  private final byte normalizedLineSeparator;

  private static final byte lineSeparator1 = TextParsingSettings.DEFAULT.getNewLineDelimiter()[0];
  private static final byte lineSeparator2 = TextParsingSettings.DEFAULT.getNewLineDelimiter().length == 2 ? TextParsingSettings.DEFAULT.getNewLineDelimiter()[1] : NULL_BYTE;;
  private static final byte normalizedLineSeparator = TextParsingSettings.DEFAULT.getNormalizedNewLine();

  private long lineCount;
  private long charCount;

  /**
   * The starting position in the file.
   */
  private final long startPos;
  private final long endPos;

  private int bufferMark;
  private long streamMark;
  private byte charMark;

  private long streamPos;

  private final FSDataInputStream input;
  private final DrillBuf buffer;
  private final ByteBuffer underlyingBuffer;

  private final boolean bufferReadable;

  /**
   * The current position in the buffer.
   */
  public int bufferPtr;

  /**
   * The quantity of valid data in the buffer.
   */
  public int length = -1;

  /**
   * Creates a new instance with the mandatory characters for handling newlines transparently.
   * @param lineSeparator the sequence of characters that represent a newline, as defined in {@link Format#getLineSeparator()}
   * @param normalizedLineSeparator the normalized newline character (as defined in {@link Format#getNormalizedNewline()}) that is used to replace any lineSeparator sequence found in the input.
   */
  public TextInput(byte[] lineSeparator, byte normalizedLineSeparator, FSDataInputStream input, DrillBuf buffer, long startPos, long endPos) {
    if (lineSeparator == null || lineSeparator.length == 0) {
      throw new IllegalArgumentException("Invalid line separator. Expected 1 to 2 characters");
    }
    if (lineSeparator.length > 2) {
      throw new IllegalArgumentException("Invalid line separator. Up to 2 characters are expected. Got " + lineSeparator.length + " characters.");
    }

    this.bufferReadable = input.getWrappedStream() instanceof ByteBufferReadable;
    this.startPos = startPos;
    this.endPos = endPos;
    this.input = input;
    this.buffer = buffer;
    this.underlyingBuffer = buffer.nioBuffer(0, buffer.capacity());

//    this.lineSeparator1 = lineSeparator[0];
//    this.lineSeparator2 = lineSeparator.length == 2 ? lineSeparator[1] : NULL_BYTE;
//    this.normalizedLineSeparator = normalizedLineSeparator;
  }

  public final void start() throws IOException {
//    stop();
    lineCount = 0;
    if(startPos > 0){
      input.seek(startPos);
    }

    updateBuffer();
    if (length > 0) {
      if(startPos > 0){
        // move to next full record.
        skipLines(1);
      }
      bufferPtr++;
    }
  }

  private void stop() throws IOException {
  }

  public String getStringSinceMarkForError() throws IOException {
    ByteArrayList bytes = new ByteArrayList();
    final long pos = getPos();
    resetToMark();
    while(getPos() < pos){
      bytes.add(nextChar());
    }
    return new String(bytes.toArray(), Charsets.UTF_8);
  }

  private long getPos(){
    return streamPos + bufferPtr;
  }

  public void mark(byte c){
    streamMark = streamPos;
    bufferMark = bufferPtr;
    charMark = c;
  }

  private final void updateBuffer() throws IOException {
    streamPos = input.getPos();
    underlyingBuffer.clear();

    if(bufferReadable){
      length = input.read(underlyingBuffer);
    }else{
      byte[] b = new byte[underlyingBuffer.capacity()];
      length = input.read(b);
      underlyingBuffer.put(b);
    }

    // make sure we haven't run over our allottment.
    if(streamPos + length > this.endPos){
      if(streamPos > this.endPos){
        length = -1;
      }else{
        length = (int) (endPos - streamPos);
      }
    }


    charCount += bufferPtr;
    bufferPtr = 0;

    buffer.writerIndex(underlyingBuffer.limit());
    buffer.readerIndex(underlyingBuffer.position());

    if (length == -1) {
      stop();
    }
  }


  public byte resetToMark() throws IOException{
    if(streamMark != streamPos){
      input.seek(streamMark);
      updateBuffer();
    }

    bufferPtr = bufferMark;
    return charMark;
  }

  public final byte nextChar() throws IOException {
    if (length == -1) {
      throw StreamFinishedPseudoException.INSTANCE;
    }


    byte byteChar = buffer.getByte(bufferPtr - 1);
//    System.out.print(byteChar);
//    System.out.print((char) (byteChar & 0xFF));


    if (bufferPtr >= length) {
      if (length != -1) {
        updateBuffer();
      } else {
        throw StreamFinishedPseudoException.INSTANCE;
      }
    }

    bufferPtr++;

    // monitor for next line.
    if (lineSeparator1 == byteChar && (lineSeparator2 == NULL_BYTE || lineSeparator2 == buffer.getByte(bufferPtr - 1))) {
      lineCount++;

      if (lineSeparator2 != NULL_BYTE) {
        byteChar = normalizedLineSeparator;

        if (bufferPtr >= length) {
          if (length != -1) {
            updateBuffer();
          } else {
            throw StreamFinishedPseudoException.INSTANCE;
          }
        }

        if (bufferPtr < length) {
          bufferPtr++;
        }
      }

      streamMark = streamPos;
      charMark = byteChar;
    }

    return byteChar;
  }

  public final long lineCount() {
    return lineCount;
  }

  public final void skipLines(int lines) throws IOException {
    if (lines < 1) {
      return;
    }
    long expectedLineCount = this.lineCount + lines;

    try {
      do {
        nextChar();
      } while (lineCount < expectedLineCount);
      if (lineCount < lines) {
        throw new IllegalArgumentException("Unable to skip " + lines + " lines from line " + (expectedLineCount - lines) + ". End of input reached");
      }
    } catch (EOFException ex) {
      throw new IllegalArgumentException("Unable to skip " + lines + " lines from line " + (expectedLineCount - lines) + ". End of input reached");
    }
  }

  public final long charCount() {
    return charCount + bufferPtr;
  }

  public void close() throws IOException{
    input.close();
  }
}
