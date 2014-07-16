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


/**
 * Represents a set of records on which a user can iterate.
 * 
 * @author Benjamin
 *
 */
public interface RecordSet extends AutoCloseable {

    /**
     * Returns the record set definition.
     * @return the record set definition.
     */
    RecordSetDefinition getRecordSetDefinition();

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
     * Returns the value of the specified field as a time stamp in seconds.
     * 
     * @param name the field name.
     * @return the value of the specified field as a time stamp in seconds.
     */
    long getTimestampInSeconds(String name);

    /**
     * Returns the value of the specified field as a time stamp in milliseconds.
     * 
     * @param index the field index.
     * @return the value of the specified field as a time stamp in milliseconds.
     */
    long getTimestampInMillis(int index);
    
    /**
     * Returns the value of the specified field as a time stamp in milliseconds.
     * 
     * @param name the field name.
     * @return the value of the specified field as a time stamp in milliseconds.
     */
    long getTimestampInMillis(String name);

    /**
     * Returns the value of the specified field as a time stamp in microseconds.
     * 
     * @param index the field index.
     * @return the value of the specified field as a time stamp in microseconds.
     */
    long getTimestampInMicros(int index);
    
    /**
     * Returns the value of the specified field as a time stamp in microseconds.
     * 
     * @param name the field name.
     * @return the value of the specified field as a time stamp in microseconds.
     */
    long getTimestampInMicros(String name);

    /**
     * Returns the value of the specified field as a time stamp in nanoseconds.
     * 
     * @param index the field index.
     * @return the value of the specified field as a time stamp in nanoseconds.
     */
    long getTimestampInNanos(int index);
    
    /**
     * Returns the value of the specified field as a time stamp in nanoseconds.
     * 
     * @param name the field name.
     * @return the value of the specified field as a time stamp in nanoseconds.
     */
    long getTimestampInNanos(String name);

    /**
     * Returns the value of the specified field as a <code>long</code>.
     * 
     * @param index the field index.
     * @return the value of the specified field as a <code>long</code>.
     */
    long getLong(int index);
    
    /**
     * Returns the value of the specified field as a <code>long</code>.
     * 
     * @param name the field name.
     * @return the value of the specified field as a <code>long</code>.
     */
    long getLong(String name);

    /**
     * Returns the value of the specified field as an <code>int</code>.
     * 
     * @param index the field index.
     * @return the value of the specified field as an <code>int</code>.
     */
    int getInt(int index);
    
    /**
     * Returns the value of the specified field as an <code>int</code>.
     * 
     * @param name the field name.
     * @return the value of the specified field as an <code>int</code>.
     */
    int getInt(String name);

    /**
     * Returns the value of the specified field as a <code>byte</code>.
     * 
     * @param index the field index.
     * @return the value of the specified field as a <code>byte</code>.
     */
    int getByte(int index);
    
    /**
     * Returns the value of the specified field as a <code>byte</code>.
     * 
     * @param name the field name.
     * @return the value of the specified field as a <code>byte</code>.
     */
    int getByte(String name);

    /**
     * Returns the value of the mantissa of the specified decimal field.
     * 
     * @param index the field index.
     * @return the value of the mantissa of the specified decimal field.
     */
    long getDecimalMantissa(int index);
    
    /**
     * Returns the value of the mantissa of the specified decimal field.
     * 
     * @param name the field name.
     * @return the value of the mantissa of the specified decimal field.
     */
    long getDecimalMantissa(String name);

    /**
     * Returns the value of the exponent of the specified decimal field.
     * 
     * @param index the field index.
     * @return the value of the exponent of the specified decimal field.
     */
    byte getDecimalExponent(int index);
    
    /**
     * Returns the value of the field as a double.
     * 
     * @param name the field name
     * @return the value of the field as a double.
     */
    double getDouble(String name);
    
    /**
     * Returns the value of the field as a double.
     * 
     * @param index the field index.
     * @return the value of the field as a double.
     */
    double getDouble(int index);
    
    /**
     * Returns the value of the exponent of the specified decimal field.
     * 
     * @param name the field name.
     * @return the value of the exponent of the specified decimal field.
     */
    byte getDecimalExponent(String name);
    
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
         * Sets the specified field to the specified <code>long</code> value. 
         * 
         * @param name the field name
         * @param l the <code>long</code> value
         * @return this <code>Builder</code>
         */
        Builder setLong(String name, long l);

        /**
         * Sets the specified field to the specified <code>int</code> value. 
         * 
         * @param index the field index
         * @param i the <code>int</code> value
         * @return this <code>Builder</code>
         */
        Builder setInt(int index, int i);
        
        /**
         * Sets the specified field to the specified <code>int</code> value. 
         * 
         * @param name the field name
         * @param i the <code>int</code> value
         * @return this <code>Builder</code>
         */
        Builder setInt(String name, int i);

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
         * @param name the field name
         * @param l the timestamp value in nanoseconds.
         * @return this <code>Builder</code>
         */
        Builder setTimestampInNanos(String name, long l);

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
         * @param name the field name
         * @param l the timestamp value in microseconds.
         * @return this <code>Builder</code>
         */
        Builder setTimestampInMicros(String name, long l);

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
         * @param name the field name
         * @param l the timestamp value in milliseconds.
         * @return this <code>Builder</code>
         */
        Builder setTimestampInMillis(String name, long l);

        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param index the field index
         * @param l the timestamp value in seconds.
         * @return this <code>Builder</code>
         */
        Builder setTimestampInSeconds(int index, long l);
        
        /**
         * Sets the specified field to the specified timestamp value. 
         * 
         * @param name the field name
         * @param l the timestamp value in seconds.
         * @return this <code>Builder</code>
         */
        Builder setTimestampInSeconds(String name, long l);

        /**
         * Sets the specified field to the specified <code>byte</code> value. 
         * 
         * @param index the field index
         * @param b the <code>byte</code> value
         * @return this <code>Builder</code>
         */
        Builder setByte(int index, int b);
        
        /**
         * Sets the specified field to the specified <code>byte</code> value. 
         * 
         * @param name the field name
         * @param b the <code>byte</code> value
         * @return this <code>Builder</code>
         */
        Builder setByte(String name, int b);

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
         * Sets the specified field to the specified decimal value. 
         * 
         * @param name the field name
         * @param mantissa the decimal mantissa
         * @param exponent the decimal exponent
         * @return this <code>Builder</code>
         */
        Builder setDecimal(String name, long mantissa, int exponent);
        
        /**
         * Sets the specified field to the specified double value. 
         * 
         * @param index the field index
         * @param d the double value
         * @return this <code>Builder</code>
         */
        Builder setDouble(int index, double d);
        
        /**
         * Sets the specified field to the specified double value. 
         * 
         * @param name the field name
         * @param d the double value
         * @return this <code>Builder</code>
         */
        Builder setDouble(String name, double d);

        /**
         * Builds a new <code>RecordSet</code> instance.
         * 
         * @return a new <code>RecordSet</code> instance.
         */
        RecordSet build();
    }
}