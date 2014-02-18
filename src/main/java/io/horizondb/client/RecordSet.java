/**
 * Copyright 2014 Benjamin Lerer
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
 */
package io.horizondb.client;

import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * Represents a set of records on which a user can iterate.
 * 
 * @author Benjamin
 *
 */
public interface RecordSet extends AutoCloseable {

    /**
     * Returns the time series definition.
     * @return the time series definition.
     */
    TimeSeriesDefinition getTimeSeriesDefinition();

    /**
     * Moves the cursor forward one record from its current position. A
     * <code>DefaultRecordSet</code> cursor is initially positioned before the first
     * record.
     * 
     * @return <code>true</code> if the new current record is valid,
     *         <code>false</code> if there are no more record
     */
    boolean next();

    /**
     * Returns this record type.
     * 
     * @return this record type.
     */
    int getType();

    /**
     * Returns the value of the specified field as a time stamp in seconds.
     * 
     * @param index the field index.
     * @return the value of the specified field as a time stamp in seconds.
     */
    long getTimestampInSeconds(int index);

    /**
     * Returns the value of the specified field as a time stamp in milliseconds.
     * 
     * @param index the field index.
     * @return the value of the specified field as a time stamp in milliseconds.
     */
    long getTimestampInMillis(int index);

    /**
     * Returns the value of the specified field as a time stamp in microseconds.
     * 
     * @param index the field index.
     * @return the value of the specified field as a time stamp in microseconds.
     */
    long getTimestampInMicros(int index);

    /**
     * Returns the value of the specified field as a time stamp in nanoseconds.
     * 
     * @param index the field index.
     * @return the value of the specified field as a time stamp in nanoseconds.
     */
    long getTimestampInNanos(int index);

    /**
     * Returns the value of the specified field as a <code>long</code>.
     * 
     * @param index the field index.
     * @return the value of the specified field as a <code>long</code>.
     */
    long getLong(int index);

    /**
     * Returns the value of the specified field as an <code>int</code>.
     * 
     * @param index the field index.
     * @return the value of the specified field as an <code>int</code>.
     */
    int getInt(int index);

    /**
     * Returns the value of the specified field as a <code>byte</code>.
     * 
     * @param index the field index.
     * @return the value of the specified field as a <code>byte</code>.
     */
    int getByte(int index);

    /**
     * Returns the value of the mantissa of the specified decimal field.
     * 
     * @param index the field index.
     * @return the value of the mantissa of the specified decimal field.
     */
    long getDecimalMantissa(int index);

    /**
     * Returns the value of the exponent of the specified decimal field.
     * 
     * @param index the field index.
     * @return the value of the exponent of the specified decimal field.
     */
    byte getDecimalExponent(int index);
    
    /**
     * {@inheritDoc}
     */
    @Override
    void close();
    
    public static interface Builder {

        /**
         * Adds a new record of the specified type.
         * 
         * @param recordType the type of record
         * @return this <code>Builder</code>
         */
        Builder newRecord(String recordType);

        /**
         * Adds a new record of the specified type.
         * 
         * @param recordTypeIndex the record type index
         * @return this <code>Builder</code>
         */
        Builder newRecord(int recordTypeIndex);

        /**
         * Sets the specified field to the specified <code>long</code> value. 
         * 
         * @param index the field index
         * @param l the <code>long</code> value
         * @return this <code>Builder</code>
         */
        Builder setLong(int index, long l);

        /**
         * Sets the specified field to the specified <code>int</code> value. 
         * 
         * @param index the field index
         * @param i the <code>int</code> value
         * @return this <code>Builder</code>
         */
        Builder setInt(int index, int i);

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in nanoseconds.
         * @return this <code>Builder</code>
         */
        Builder setTimestampInNanos(int index, long l);

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in microseconds.
         * @return this <code>Builder</code>
         */
        Builder setTimestampInMicros(int index, long l);

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in milliseconds.
         * @return this <code>Builder</code>
         */
        Builder setTimestampInMillis(int index, long l);

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in seconds.
         * @return this <code>Builder</code>
         */
        Builder setTimestampInSeconds(int index, long l);

        /**
         * Sets the specified field to the specified <code>byte</code> value. 
         * 
         * @param index the field index
         * @param b the <code>byte</code> value
         * @return this <code>Builder</code>
         */
        Builder setByte(int index, int b);

        /**
         * Sets the specified field to the specified decimal value. 
         * 
         * @param index the field index
         * @param mantissa the decimal mantissa
         * @param exponent the decimal exponent
         * @return this <code>Builder</code>
         */
        Builder setDecimal(int index, long mantissa, int exponent);
        
        /**
         * Sets the specified field to the specified double value. 
         * 
         * @param index the field index
         * @param d the double value
         * @return this <code>Builder</code>
         */
        Builder setDouble(int index, double d);

        /**
         * Builds a new <code>RecordSet</code> instance.
         * 
         * @return a new <code>RecordSet</code> instance.
         */
        RecordSet build();
    }
}