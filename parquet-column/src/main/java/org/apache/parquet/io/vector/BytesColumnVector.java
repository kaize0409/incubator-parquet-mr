/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.io.vector;

import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.io.ColumnVector;

public class BytesColumnVector extends ColumnVector {
  public final byte[][] values;
  private byte[] buffer;   // optional buffer to use when actually copying in data
  public int[] start;          // start offset of each field
  public int[] length;
  private int nextFree;    // next free position in buffer

  // Estimate that there will be 16 bytes per entry
  static final int DEFAULT_BUFFER_SIZE = 16 * MAX_VECTOR_LENGTH;

  // Proportion of extra space to provide when allocating more buffer space.
  static final float EXTRA_SPACE_FACTOR = (float) 1.2;

  public BytesColumnVector() {
    this.valueType = byte.class;
    values = new byte[MAX_VECTOR_LENGTH][];
    start = new int[MAX_VECTOR_LENGTH];
    length = new int[MAX_VECTOR_LENGTH];
    initBuffer(0);
  }

  @Override
  public void doReadFrom(ColumnReader reader, int index) {
    byte[] bytes = reader.getBinary().getBytesUnsafe();
    setVal(index, bytes, 0, bytes.length);
  }

  /**
   * Set a field by actually copying in to a local buffer.
   * If you must actually copy data in to the array, use this method.
   * DO NOT USE this method unless it's not practical to set data by reference with setRef().
   * Setting data by reference tends to run a lot faster than copying data in.
   *
   * @param elementNum index within column vector to set
   * @param sourceBuf container of source data
   * @param start start byte position within source
   * @param length  length of source byte sequence
   */
  public void setVal(int elementNum, byte[] sourceBuf, int start, int length) {
    if ((nextFree + length) > buffer.length) {
      increaseBufferSpace(length);
    }
    System.arraycopy(sourceBuf, start, buffer, nextFree, length);
    values[elementNum] = buffer;
    this.start[elementNum] = nextFree;
    this.length[elementNum] = length;
    nextFree += length;
  }

  /**
   * Increase buffer space enough to accommodate next element.
   * This uses an exponential increase mechanism to rapidly
   * increase buffer size to enough to hold all data.
   * As batches get re-loaded, buffer space allocated will quickly
   * stabilize.
   *
   * @param nextElemLength size of next element to be added
   */
  public void increaseBufferSpace(int nextElemLength) {

    // Keep doubling buffer size until there will be enough space for next element.
    int newLength = 2 * buffer.length;
    while((nextFree + nextElemLength) > newLength) {
      newLength *= 2;
    }

    // Allocate new buffer, copy data to it, and set buffer to new buffer.
    byte[] newBuffer = new byte[newLength];
    System.arraycopy(buffer, 0, newBuffer, 0, nextFree);
    buffer = newBuffer;
  }

  /**
   * You must call initBuffer first before using setVal().
   * Provide the estimated number of bytes needed to hold
   * a full column vector worth of byte string data.
   *
   * @param estimatedValueSize Estimated size of buffer space needed
   */
  public void initBuffer(int estimatedValueSize) {
    nextFree = 0;

    // if buffer is already allocated, keep using it, don't re-allocate
    if (buffer != null) {
      return;
    }

    // allocate a little extra space to limit need to re-allocate
    int bufferSize = this.values.length * (int) (estimatedValueSize * EXTRA_SPACE_FACTOR);
    if (bufferSize < DEFAULT_BUFFER_SIZE) {
      bufferSize = DEFAULT_BUFFER_SIZE;
    }
    buffer = new byte[bufferSize];
  }
}
