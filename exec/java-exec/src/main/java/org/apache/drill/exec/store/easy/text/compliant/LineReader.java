package org.apache.drill.exec.store.easy.text.compliant;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DrillBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.internal.PlatformDependent;

import java.io.IOException;

import org.apache.drill.exec.util.AssertionUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;

public class LineReader {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LineReader.class);

  private static final int MEMMOVE_SIZE = 256;

  public static enum Outcome {
    UNKNOWN_ERROR(-1), LAST_RECORD(-2);
    public final int code;
    Outcome(int code){
      this.code = code;
    }
  }

  private final byte lineSeparator1;
  private final byte lineSeparator2;
  private final DrillBuf buffer;
  private final long bufStart;
  private final long bufEnd;
  private final long startPos;
  private final long endPos;
  private final String fileName; // for debugging purposes only.

  private static final int LAST_RECORD = -5;

  private int previousEnd;
  private long currentStreamPosition;
  private int bytesAvailable;


  public LineReader(String fileName, DrillBuf buffer, FSDataInputStream in, long startPos, long endPos, byte lineSeparator1, byte lineSeparator2){
    this.buffer = buffer;
    this.bufStart = buffer.memoryAddress();
    this.bufEnd = buffer.memoryAddress() + buffer.capacity();
    this.startPos = startPos;
    this.endPos = endPos;
    this.lineSeparator1 = lineSeparator1;
    this.lineSeparator2 = lineSeparator2;
    this.fileName = fileName;
  }

  private void checkBuffer(){
    if(AssertionUtil.BOUNDS_CHECKING_ENABLED){
      buffer.checkBytes(0, buffer.capacity());
    }
  }

  /**
   * Returns the next line ending in the shared buffer.  A negative return indicates that more information should be reviewed.
   * @return
   * @throws IOException
   */
  public int getNextLineEndingOne() throws IOException{
    final byte lineSeparator1 = this.lineSeparator1;
    checkBuffer();


    final long max = this.bufStart + this.bytesAvailable;
    int nextEnd = -1;
    outside: while(true){
      for(long pos = bufStart + previousEnd; pos < max; pos++){

        if(lineSeparator1 == PlatformDependent.getByte(pos)){
          nextEnd = (int) (pos - bufStart);
          break outside;
        }
      }

      if(nextEnd == -1){
        // we were unable to find a separator.

        if(previousEnd == 0){
          throw fail("Unable to find a record delimiter within window.");
        }

        // copy data to left and load more data.
        slideLeft(bufStart, bufStart + previousEnd, bytesAvailable - previousEnd);
        int dataRead = readMoreData(bytesAvailable - previousEnd);
        if(dataRead == 0){
          previousEnd = nextEnd;
          return Outcome.LAST_RECORD.code;
        }
      }
    }

    return nextEnd;
  }

  private static final void slideLeftIntermediate(final long origin, final long rangeStart, final int rangeLength){
    if(rangeStart <= origin){
      throw new IllegalArgumentException(String.format("Addresses were invalid.  To slide left, the origin of %d must be less than the range start of %d.", origin, rangeStart));
    }else{
      // not enough overlap, we'll double copy.
      ByteBuf buf = Unpooled.directBuffer(rangeLength);
      long intermediate = buf.memoryAddress();
      PlatformDependent.copyMemory(rangeStart, intermediate, rangeLength);
      PlatformDependent.copyMemory(intermediate, origin, rangeLength);
      buf.release();
    }
  }

  private static final void slideLeft(final long origin, final long rangeStart, final int rangeLength){

    long nonOverlap = rangeStart - origin;

    if(nonOverlap < MEMMOVE_SIZE){
      slideLeftIntermediate(origin, rangeStart, rangeLength);
    }else{
      int offset;
      for(offset = 0; offset + MEMMOVE_SIZE < rangeLength; offset += MEMMOVE_SIZE){
        PlatformDependent.copyMemory(rangeStart+offset, origin+offset, MEMMOVE_SIZE);
      }
      int rangeRemainder = rangeLength - offset;
      if(rangeRemainder > 0){
        PlatformDependent.copyMemory(rangeStart + offset, origin+offset, rangeRemainder);
      }
    }
  }


  private int readMoreData(int startingPosition) throws IOException {

  }

  public int getNextLineEndingTwo(){
    final byte lineSeparator1 = this.lineSeparator1;
    final byte lineSeparator2 = this.lineSeparator2;

    buffer.checkBytes(previousEnd, buffer.capacity());

    final long max = buffer.memoryAddress() + buffer.capacity();
    int nextEnd = -1;
    for(long pos = buffer.memoryAddress() + previousEnd; pos < max; pos++){

      if(lineSeparator1 == PlatformDependent.getByte(pos)){
        long posPlusOne = pos+1;
        if(posPlusOne < max){
          if(lineSeparator2 == PlatformDependent.getByte(posPlusOne)){
            nextEnd = (int) (posPlusOne - buffer.memoryAddress());
          }
        }else{
          // there isn't enough data in the buffer so we need to shift previousEnd to current end to the start of the buffer and refill the buffer.
        }
        nextEnd = (int) (pos - buffer.memoryAddress());
        break;
      }
    }

    return nextEnd;
  }

  private IOException fail(String message, Object...objects ) throws IOException{
    long windowStart = this.currentStreamPosition - bytesAvailable + previousEnd;
    String compoundMessage = String.format("Failure reading file: %s.  Error was %s.  Current stream was working on the window %d..%d.", fileName, message, windowStart, currentStreamPosition);
    return new IOException(String.format(compoundMessage, objects));
  }
}
