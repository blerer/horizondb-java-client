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

import io.horizondb.client.RecordSet.Builder;
import io.horizondb.model.core.RecordListMultimapBuilder;
import io.horizondb.model.schema.TimeSeriesDefinition;

/**
 * Default <code>RecordSet.Builder </code>.
 * 
 * @author Benjamin
 *
 */
final class RecordSetBuilder implements RecordSet.Builder {

    /**
     * The Multimap build.
     */
    private final RecordListMultimapBuilder builder;

    /**
     * The time series definition.
     */
    private final TimeSeriesDefinition definition;
    
    
    /**
     * 
     * 
     * @param builder
     */
    public RecordSetBuilder(TimeSeriesDefinition definition) {
        
        this.definition = definition;
        this.builder = new RecordListMultimapBuilder(definition);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final RecordSetBuilder newRecord(String recordType) {
        this.builder.newRecord(recordType);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final RecordSetBuilder newRecord(int recordTypeIndex) {
        this.builder.newRecord(recordTypeIndex);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final RecordSetBuilder setLong(int index, long l) {
        this.builder.setLong(index, l);
        return this;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Builder setLong(String name, long l) {
        this.builder.setLong(name, l);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final RecordSetBuilder setInt(int index, int i) {
        this.builder.setInt(index, i);
        return this;
    }

    /**
     * {@inheritDoc}
     */    
    @Override
    public Builder setInt(String name, int i) {
        this.builder.setInt(name, i);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Builder setDouble(String name, double d) {
        this.builder.setDouble(name, d);
        return this;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Builder setDouble(int index, double d) {
        this.builder.setDouble(index, d);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final RecordSetBuilder setTimestampInNanos(int index, long l) {
        this.builder.setTimestampInNanos(index, l);
        return this;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Builder setTimestampInNanos(String name, long l) {
        this.builder.setTimestampInNanos(name, l);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final RecordSetBuilder setTimestampInMicros(int index, long l) {
        this.builder.setTimestampInMicros(index, l);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final RecordSetBuilder setTimestampInMillis(int index, long l) {
        this.builder.setTimestampInMillis(index, l);
        return this;
    }


    /**    
     * {@inheritDoc}
     */
    @Override
    public Builder setTimestampInMillis(String name, long l) {
        this.builder.setTimestampInMillis(name, l);
        return this;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final RecordSetBuilder setTimestampInSeconds(int index, long l) {
        this.builder.setTimestampInSeconds(index, l);
        return this;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Builder setTimestampInMicros(String name, long l) {
        this.builder.setTimestampInMicros(name, l);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Builder setTimestampInSeconds(String name, long l) {
        this.builder.setTimestampInSeconds(name, l);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final RecordSetBuilder setByte(int index, int b) {
        this.builder.setByte(index, b);
        return this;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Builder setByte(String name, int b) {
        this.builder.setByte(name, b);
        return this;
    }

    /**    
     * {@inheritDoc}
     */
    @Override
    public Builder setDecimal(String name, long mantissa, int exponent) {
        this.builder.setDecimal(name, mantissa, exponent);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final RecordSetBuilder setDecimal(int index, long mantissa, int exponent) {
        this.builder.setDecimal(index, mantissa, exponent);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final RecordSet build() {
        return new PartitionAwareRecordSet(this.definition, this.builder.buildMultimap());
    }
}
