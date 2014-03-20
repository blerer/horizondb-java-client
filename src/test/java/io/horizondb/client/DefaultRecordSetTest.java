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
import io.horizondb.model.schema.DatabaseDefinition;
import io.horizondb.model.schema.FieldType;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 *
 */
public class DefaultRecordSetTest {

	private TimeSeriesDefinition definition;
	
	@Before
	public void setUp() {
		
		RecordTypeDefinition recordType = RecordTypeDefinition.newBuilder("ExchangeState")
		                                                      .addMillisecondTimestampField("exchangeTimestamp")
		                                                      .addByteField("status")
		                                                      .build();
		
		DatabaseDefinition databaseDefinition = new DatabaseDefinition("test");
		
		this.definition = databaseDefinition.newTimeSeriesDefinitionBuilder("test")
		                                    .timeUnit(TimeUnit.NANOSECONDS)
		                                    .addRecordType(recordType)
		                                    .build();
	}
	
	@After
	public void tearDown() {
		
		this.definition = null;
	}
	
	@Test
	public void testWithFullRecords() {
		
		TimeSeriesRecord first = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		first.setTimestampInNanos(0, 12000700);
		first.setTimestampInMillis(1, 12);
		first.setByte(2, 3);

		TimeSeriesRecord second = new TimeSeriesRecord(0,
		                                               TimeUnit.NANOSECONDS,
		                                               FieldType.MILLISECONDS_TIMESTAMP,
		                                               FieldType.BYTE);
		second.setTimestampInNanos(0, 13000900);
		second.setTimestampInMillis(1, 13);
		second.setByte(2, 3);

		TimeSeriesRecord third = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		third.setTimestampInNanos(0, 13004400);
		third.setTimestampInMillis(1, 13);
		third.setByte(2, 1);

		RecordIterator iterator = new RecordIteratorStub(asList(first, second, third));
		
		try (RecordSet defaultRecordSet = new DefaultRecordSet(this.definition, iterator)) {

			assertTrue(defaultRecordSet.next());

			assertEquals(first.getTimestampInNanos(0), defaultRecordSet.getTimestampInNanos(0));
			assertEquals(first.getTimestampInMillis(1), defaultRecordSet.getTimestampInMillis(1));
			assertEquals(first.getByte(2), defaultRecordSet.getByte(2));

			assertTrue(defaultRecordSet.next());

			assertEquals(second.getTimestampInNanos(0), defaultRecordSet.getTimestampInNanos(0));
			assertEquals(second.getTimestampInMillis(1), defaultRecordSet.getTimestampInMillis(1));
			assertEquals(second.getByte(2), defaultRecordSet.getByte(2));

			assertTrue(defaultRecordSet.next());

			assertEquals(third.getTimestampInNanos(0), defaultRecordSet.getTimestampInNanos(0));
			assertEquals(third.getTimestampInMillis(1), defaultRecordSet.getTimestampInMillis(1));
			assertEquals(third.getByte(2), defaultRecordSet.getByte(2));

			assertFalse(defaultRecordSet.next());
		}
	}

	@Test
	public void testWithDeltas() {
		
		TimeSeriesRecord first = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		first.setTimestampInNanos(0, 12000700);
		first.setTimestampInMillis(1, 12);
		first.setByte(2, 3);

		TimeSeriesRecord second = new TimeSeriesRecord(0,
		                                               TimeUnit.NANOSECONDS,
		                                               FieldType.MILLISECONDS_TIMESTAMP,
		                                               FieldType.BYTE);
		second.setDelta(true);
		second.setTimestampInNanos(0, 1000200);
		second.setTimestampInMillis(1, 1);

		TimeSeriesRecord third = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		third.setDelta(true);
		third.setTimestampInNanos(0, 3500);
		third.setByte(2, -2);

		RecordIterator iterator = new RecordIteratorStub(asList(first, second, third));
		
		try (RecordSet defaultRecordSet = new DefaultRecordSet(this.definition, iterator)) {

			assertTrue(defaultRecordSet.next());

			assertEquals(first.getTimestampInNanos(0), defaultRecordSet.getTimestampInNanos(0));
			assertEquals(first.getTimestampInMillis(1), defaultRecordSet.getTimestampInMillis(1));
			assertEquals(first.getByte(2), defaultRecordSet.getByte(2));

			assertTrue(defaultRecordSet.next());
			
			assertEquals(13000900, defaultRecordSet.getTimestampInNanos(0));
			assertEquals(13, defaultRecordSet.getTimestampInMillis(1));
			assertEquals(3, defaultRecordSet.getByte(2));

			assertTrue(defaultRecordSet.next());
			
			assertEquals(13004400, defaultRecordSet.getTimestampInNanos(0));
			assertEquals(13, defaultRecordSet.getTimestampInMillis(1));
			assertEquals(1, defaultRecordSet.getByte(2));

			assertFalse(defaultRecordSet.next());
		}
	}
	
	@Test
	public void testWithDeltasAndFullState() {
		
		TimeSeriesRecord first = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		first.setTimestampInNanos(0, 12000700);
		first.setTimestampInMillis(1, 12);
		first.setByte(2, 3);

		TimeSeriesRecord second = new TimeSeriesRecord(0,
		                                               TimeUnit.NANOSECONDS,
		                                               FieldType.MILLISECONDS_TIMESTAMP,
		                                               FieldType.BYTE);
		second.setDelta(true);
		second.setTimestampInNanos(0, 1000200);
		second.setTimestampInMillis(1, 1);

		TimeSeriesRecord third = new TimeSeriesRecord(0,
		                                              TimeUnit.NANOSECONDS,
		                                              FieldType.MILLISECONDS_TIMESTAMP,
		                                              FieldType.BYTE);
		third.setTimestampInNanos(0, 13004400);
		third.setTimestampInMillis(1, 13);
		third.setByte(2, 1);

		RecordIterator iterator = new RecordIteratorStub(asList(first, second, third));
		
		try (RecordSet defaultRecordSet = new DefaultRecordSet(this.definition, iterator)) {

			assertTrue(defaultRecordSet.next());

			assertEquals(first.getTimestampInNanos(0), defaultRecordSet.getTimestampInNanos(0));
			assertEquals(first.getTimestampInMillis(1), defaultRecordSet.getTimestampInMillis(1));
			assertEquals(first.getByte(2), defaultRecordSet.getByte(2));

			assertTrue(defaultRecordSet.next());
			
			assertEquals(13000900, defaultRecordSet.getTimestampInNanos(0));
			assertEquals(13, defaultRecordSet.getTimestampInMillis(1));
			assertEquals(3, defaultRecordSet.getByte(2));

			assertTrue(defaultRecordSet.next());
			
			assertEquals(13004400, defaultRecordSet.getTimestampInNanos(0));
			assertEquals(13, defaultRecordSet.getTimestampInMillis(1));
			assertEquals(1, defaultRecordSet.getByte(2));

			assertFalse(defaultRecordSet.next());
		}
	}
	
	private static class RecordIteratorStub implements RecordIterator {

		private final Iterator<? extends Record> iterator;
		
		public RecordIteratorStub(Iterable<? extends Record> iterable) {
			this.iterator = iterable.iterator();
		}
		
		/**
		 * {@inheritDoc}
		 */
        @Override
        public void close() throws IOException {

        }

		/**
		 * {@inheritDoc}
		 */
        @Override
        public boolean hasNext() throws IOException {
	        return this.iterator.hasNext();
        }

		/**
		 * {@inheritDoc}
		 */
        @Override
        public Record next() throws IOException {
        	return this.iterator.next();
        }
		
	}
}
