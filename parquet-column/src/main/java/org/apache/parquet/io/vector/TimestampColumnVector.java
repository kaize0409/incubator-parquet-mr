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

import jodd.datetime.JDateTime;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.io.ColumnVector;
import org.apache.parquet.io.api.Binary;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * Created by root on 7/26/16.
 */
public class TimestampColumnVector extends ColumnVector {
  static final long NANOS_PER_HOUR = TimeUnit.HOURS.toNanos(1);
  static final long NANOS_PER_MINUTE = TimeUnit.MINUTES.toNanos(1);
  static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);
  static final long NANOS_PER_DAY = TimeUnit.DAYS.toNanos(1);

  private static final ThreadLocal<Calendar> parquetGMTCalendar = new ThreadLocal<Calendar>();
  private static final ThreadLocal<Calendar> parquetLocalCalendar = new ThreadLocal<Calendar>();

  private boolean skipTimestampConversion = false;
  /*
   * The storage arrays for this column vector corresponds to the storage of a Timestamp:
   */
  public long[] time;
  // The values from Timestamp.getTime().

  public int[] nanos;
  // The values from Timestamp.getNanos().

  public TimestampColumnVector(boolean skipTimestampConversion){
    this.skipTimestampConversion = skipTimestampConversion;

    time = new long[MAX_VECTOR_LENGTH];
    nanos = new int[MAX_VECTOR_LENGTH];
  }

  @Override
  protected void doReadFrom(
    ColumnReader reader,
    int index) {
    Binary bytes = reader.getBinary();
    ByteBuffer buf = bytes.toByteBuffer();
    buf.order(ByteOrder.LITTLE_ENDIAN);
    long timeOfDayNanos = buf.getLong();
    int julianDay = buf.getInt();
    NanoTime nt = new NanoTime(julianDay, timeOfDayNanos);
    Timestamp ts = getTimestamp(nt, skipTimestampConversion);
    time[index] = ts.getTime();
    nanos[index] = ts.getNanos();
  }

  public static Timestamp getTimestamp(
    NanoTime nt,
    boolean skipConversion) {
    int julianDay = nt.getJulianDay();
    long nanosOfDay = nt.getTimeOfDayNanos();

    long remainder = nanosOfDay;
    julianDay += remainder / NANOS_PER_DAY;
    remainder %= NANOS_PER_DAY;
    if (remainder < 0) {
      remainder += NANOS_PER_DAY;
      julianDay--;
    }

    JDateTime jDateTime = new JDateTime((double) julianDay);
    Calendar calendar = getCalendar(skipConversion);
    calendar.set(Calendar.YEAR, jDateTime.getYear());
    calendar.set(Calendar.MONTH, jDateTime.getMonth() - 1); //java calendar index starting at 1.
    calendar.set(Calendar.DAY_OF_MONTH, jDateTime.getDay());

    int hour = (int) (remainder / (NANOS_PER_HOUR));
    remainder = remainder % (NANOS_PER_HOUR);
    int minutes = (int) (remainder / (NANOS_PER_MINUTE));
    remainder = remainder % (NANOS_PER_MINUTE);
    int seconds = (int) (remainder / (NANOS_PER_SECOND));
    long nanos = remainder % NANOS_PER_SECOND;

    calendar.set(Calendar.HOUR_OF_DAY, hour);
    calendar.set(Calendar.MINUTE, minutes);
    calendar.set(Calendar.SECOND, seconds);
    Timestamp ts = new Timestamp(calendar.getTimeInMillis());
    ts.setNanos((int) nanos);
    return ts;
  }

  private static Calendar getCalendar(boolean skipConversion) {
    Calendar calendar = skipConversion ? getLocalCalendar() : getGMTCalendar();
    calendar.clear(); // Reset all fields before reusing this instance
    return calendar;
  }

  private static Calendar getGMTCalendar() {
    //Calendar.getInstance calculates the current-time needlessly, so cache an instance.
    if (parquetGMTCalendar.get() == null) {
      parquetGMTCalendar.set(Calendar.getInstance(TimeZone.getTimeZone("GMT")));
    }
    return parquetGMTCalendar.get();
  }

  private static Calendar getLocalCalendar() {
    if (parquetLocalCalendar.get() == null) {
      parquetLocalCalendar.set(Calendar.getInstance());
    }
    return parquetLocalCalendar.get();
  }
}
