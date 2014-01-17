/**
 * Copyright 2013 Benjamin Lerer
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

import io.horizondb.model.core.Record;
import io.horizondb.model.core.RecordIterator;
import io.horizondb.model.core.records.TimeSeriesRecord;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;

import static org.apache.commons.lang.Validate.isTrue;

/**
 * default implementation of <code>RecordSet</code>.
 * 
 * @author Benjamin
 * 
 */
class DefaultRecordSet implements RecordSet {

	/**
	 * The definition of the time series targeted 
	 */
	private final TimeSeriesDefinition definition;
	
	/**
	 * The current record being exposed.
	 */
	private TimeSeriesRecord current;

	/**
	 * The time series records for each types.
	 */
	private final TimeSeriesRecord[] records;

	/**
	 * The record iterator used by this <code>DefaultRecordSet</code>.
	 */
	private final RecordIterator iterator;

	/**
	 * Creates a new <code>DefaultRecordSet</code> for the specified time series that will iterate over the specified 
	 * records.
	 * 
	 * @param definition the time series definition
	 * @param iterator the record iterator
	 */
	DefaultRecordSet(TimeSeriesDefinition definition, RecordIterator iterator) {

		this.definition = definition;
		this.records = definition.newRecords();
		this.iterator = iterator;
	}

    /**
     * <code>true</code> if the end of the record set has been reached.
     */
    protected boolean endOfRecordSet;

    /**
     * <code>true</code> if the record set has been closed.
     */
    protected boolean closed;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final int getType() {
        checkState();
        return this.current.getType();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getTimestampInSeconds(int index) {
        checkState();
        return this.current.getTimestampInSeconds(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getTimestampInMillis(int index) {
        checkState();
        return this.current.getTimestampInMillis(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getTimestampInMicros(int index) {
        checkState();
        return this.current.getTimestampInMicros(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getTimestampInNanos(int index) {
        checkState();
        return this.current.getTimestampInNanos(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getLong(int index) {
        checkState();
        return this.current.getLong(index);
     }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int getInt(int index) {
        checkState();
        return this.current.getInt(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final int getByte(int index) {
        checkState();
        return this.current.getByte(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final long getDecimalMantissa(int index) {
        checkState();
        return this.current.getDecimalMantissa(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final byte getDecimalExponent(int index) {
        checkState();
        return this.current.getDecimalExponent(index);
    }
    
    /**
     * Checks that this record set is in a valid state for reading fields.
     */
    private void checkState() {

        isTrue(!this.closed, "The RecordSet has been closed.");
        isTrue(!this.endOfRecordSet, "All the records of the RecordSet has been read.");
        isTrue(this.current != null, "The next method must be called before trying to read the record fields.");
    }
	
	/**
     * {@inheritDoc}
     */
	@Override
    public final TimeSeriesDefinition getTimeSeriesDefinition() {
		return this.definition;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public final void close() {

		if (this.closed) {
			return;
		}

		this.closed = true;

		try {

			this.iterator.close();

		} catch (IOException e) {
			throw new HorizonDBException("", e);
		}
	}

	/**
     * {@inheritDoc}
     */
	@Override
    public final boolean next() {
	    
		try {

			this.endOfRecordSet = !this.iterator.hasNext();

			if (this.endOfRecordSet) {
				return false;
			}

			Record next = this.iterator.next();

			this.current = this.records[next.getType()];

			if (next.isDelta()) {

				this.current.add(next);

			} else {

				next.copyTo(this.current);
			}

	        onNext();
			return true;

		} catch (IOException e) {

			this.endOfRecordSet = true;
			throw new HorizonDBException("", e);
		}
	}

    /**
     * Notification that next has been called. 
     */
    protected void onNext() {

    }
}
