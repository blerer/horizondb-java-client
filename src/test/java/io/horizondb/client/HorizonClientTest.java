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

import io.horizondb.db.Configuration;
import io.horizondb.db.HorizonServer;
import io.horizondb.io.files.FileUtils;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.RecordTypeDefinition;
import io.horizondb.model.schema.TimeSeriesDefinition;
import io.horizondb.test.AssertFiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Benjamin
 * 
 */
public class HorizonClientTest {

    private Path testDirectory;

    @Before
    public void setUp() throws IOException {
        this.testDirectory = Files.createTempDirectory("test");
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.forceDelete(this.testDirectory);
    }

    @Test
    public void testGetDatabaseWithNotExistingDatabase() throws Exception {

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.getDatabase("test");
                Assert.fail();

            } catch (HorizonDBException e) {

                Assert.assertTrue(true);
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testCreateDatabase() throws Exception {

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.newDatabase("test");
            }

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                Database database = client.getDatabase("Test");
                Assert.assertEquals("test", database.getName());
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testCreateDatabaseWithInvalidName() throws Exception {

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.setQueryTimeoutInSeconds(3);
                client.newDatabase(" ");
                Assert.fail();

            } catch (HorizonDBException e) {

                Assert.assertTrue(true);
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testCreateTimeSeries() throws Exception {

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                Database database = client.newDatabase("test");

                RecordTypeDefinition quote = RecordTypeDefinition.newBuilder("Quote")
                                                                 .addDecimalField("bestBid")
                                                                 .addDecimalField("bestAsk")
                                                                 .addIntegerField("bidVolume")
                                                                 .addIntegerField("askVolume")
                                                                 .build();

                TimeSeriesDefinition definition = database.newTimeSeriesDefinitionBuilder("DAX")
                                                          .timeUnit(TimeUnit.NANOSECONDS)
                                                          .addRecordType(quote)
                                                          .build();

                database.createTimeSeries(definition);
            }

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                TimeSeries timeSeries = client.getDatabase("Test").getTimeSeries("DAX");
                Assert.assertEquals("DAX", timeSeries.getName());
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testGetTimeSeriesWithNotExistingSeries() throws Exception {

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.newDatabase("test");
            }

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                Database database = client.getDatabase("test");
                database.getTimeSeries("DAX30");

                Assert.fail();

            } catch (HorizonDBException e) {

                Assert.assertTrue(true);
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testInsertIntoTimeSeries() throws Exception {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.setQueryTimeoutInSeconds(120);
                Database database = client.newDatabase("test");

                RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                                 .addDecimalField("price")
                                                                 .addLongField("volume")
                                                                 .build();

                TimeSeriesDefinition definition = database.newTimeSeriesDefinitionBuilder("DAX")
                                                          .timeUnit(TimeUnit.MILLISECONDS)
                                                          .addRecordType(trade)
                                                          .build();

                TimeSeries timeSeries = database.createTimeSeries(definition);

                RecordSet timeSeriesRecordSet = timeSeries.newRecordSetBuilder()
                                                          .newRecord("Trade")
                                                          .setTimestampInMillis(0, timestamp)
                                                          .setDecimal(1, 125, -1)
                                                          .setLong(2, 10)
                                                          .newRecord("Trade")
                                                          .setTimestampInMillis(0, timestamp + 100)
                                                          .setDecimal(1, 12, 0)
                                                          .setLong(2, 5)
                                                          .newRecord("Trade")
                                                          .setTimestampInMillis(0, timestamp + 350)
                                                          .setDecimal(1, 11, 0)
                                                          .setLong(2, 10)
                                                          .build();

                timeSeries.write(timeSeriesRecordSet);

                RecordSet defaultRecordSet = timeSeries.read(timestamp, timestamp + 20000);

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(125, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(10, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp + 100, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(120, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(5, defaultRecordSet.getLong(2));
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testInsertIntoTimeSeriesWithReplay() throws Exception {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.setQueryTimeoutInSeconds(120);
                Database database = client.newDatabase("test");

                RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                                 .addDecimalField("price")
                                                                 .addLongField("volume")
                                                                 .build();

                TimeSeriesDefinition definition = database.newTimeSeriesDefinitionBuilder("DAX")
                                                          .timeUnit(TimeUnit.MILLISECONDS)
                                                          .addRecordType(trade)
                                                          .build();

                TimeSeries timeSeries = database.createTimeSeries(definition);

                RecordSet timeSeriesRecordSet = timeSeries.newRecordSetBuilder()
                                                          .newRecord("Trade")
                                                          .setTimestampInMillis(0, timestamp)
                                                          .setDecimal(1, 125, -1)
                                                          .setLong(2, 10)
                                                          .newRecord("Trade")
                                                          .setTimestampInMillis(0, timestamp + 100)
                                                          .setDecimal(1, 12, 0)
                                                          .setLong(2, 5)
                                                          .newRecord("Trade")
                                                          .setTimestampInMillis(0, timestamp + 350)
                                                          .setDecimal(1, 11, 0)
                                                          .setLong(2, 10)
                                                          .build();

                timeSeries.write(timeSeriesRecordSet);

                RecordSet defaultRecordSet = timeSeries.read(timestamp, timestamp + 20000);

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(125, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(10, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp + 100, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(120, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(5, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp + 350, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(110, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(10, defaultRecordSet.getLong(2));

                assertFalse(defaultRecordSet.next());
            }

        } finally {

            server.shutdown();
        }

        AssertFiles.assertFileExists(this.testDirectory.resolve("data").resolve("test").resolve("DAX-1384383600000.ts"));

        server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.setQueryTimeoutInSeconds(120);
                TimeSeries timeSeries = client.getDatabase("test").getTimeSeries("DAX");

                RecordSet defaultRecordSet = timeSeries.read(timestamp, timestamp + 20000);

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(125, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(10, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp + 100, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(120, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(5, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp + 350, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(110, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(10, defaultRecordSet.getLong(2));

                assertFalse(defaultRecordSet.next());
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testInsertIntoTimeSeriesWithWithCommitLogSegmentSwitchAndForceFlush() throws Exception {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .commitLogSegmentSize(200)
                                                   .maximumNumberOfCommitLogSegments(3)
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.setQueryTimeoutInSeconds(120);
                Database database = client.newDatabase("test");

                RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                                 .addDecimalField("price")
                                                                 .addLongField("volume")
                                                                 .build();

                TimeSeriesDefinition definition = database.newTimeSeriesDefinitionBuilder("DAX")
                                                          .timeUnit(TimeUnit.MILLISECONDS)
                                                          .addRecordType(trade)
                                                          .build();

                TimeSeries timeSeries = database.createTimeSeries(definition);

                RecordSet timeSeriesRecordSet = timeSeries.newRecordSetBuilder()
                                                          .newRecord("Trade")
                                                          .setTimestampInMillis(0, timestamp)
                                                          .setDecimal(1, 125, -1)
                                                          .setLong(2, 10)
                                                          .newRecord("Trade")
                                                          .setTimestampInMillis(0, timestamp + 100)
                                                          .setDecimal(1, 12, 0)
                                                          .setLong(2, 5)
                                                          .newRecord("Trade")
                                                          .setTimestampInMillis(0, timestamp + 350)
                                                          .setDecimal(1, 11, 0)
                                                          .setLong(2, 10)
                                                          .build();

                timeSeries.write(timeSeriesRecordSet);

                timeSeriesRecordSet = timeSeries.newRecordSetBuilder()
                                                .newRecord("Trade")
                                                .setTimestampInMillis(0, timestamp + 400)
                                                .setDecimal(1, 125, -1)
                                                .setLong(2, 10)
                                                .newRecord("Trade")
                                                .setTimestampInMillis(0, timestamp + 500)
                                                .setDecimal(1, 12, 0)
                                                .setLong(2, 5)
                                                .newRecord("Trade")
                                                .setTimestampInMillis(0, timestamp + 650)
                                                .setDecimal(1, 11, 0)
                                                .setLong(2, 10)
                                                .build();

                timeSeries.write(timeSeriesRecordSet);

                timeSeriesRecordSet = timeSeries.newRecordSetBuilder()
                                                .newRecord("Trade")
                                                .setTimestampInMillis(0, timestamp + 800)
                                                .setDecimal(1, 125, -1)
                                                .setLong(2, 10)
                                                .newRecord("Trade")
                                                .setTimestampInMillis(0, timestamp + 850)
                                                .setDecimal(1, 12, 0)
                                                .setLong(2, 5)
                                                .newRecord("Trade")
                                                .setTimestampInMillis(0, timestamp + 900)
                                                .setDecimal(1, 11, 0)
                                                .setLong(2, 10)
                                                .build();

                timeSeries.write(timeSeriesRecordSet);

                timeSeriesRecordSet = timeSeries.newRecordSetBuilder()
                                                .newRecord("Trade")
                                                .setTimestampInMillis(0, timestamp + 1000)
                                                .setDecimal(1, 125, -1)
                                                .setLong(2, 10)
                                                .newRecord("Trade")
                                                .setTimestampInMillis(0, timestamp + 1050)
                                                .setDecimal(1, 12, 0)
                                                .setLong(2, 5)
                                                .newRecord("Trade")
                                                .setTimestampInMillis(0, timestamp + 1200)
                                                .setDecimal(1, 11, 0)
                                                .setLong(2, 10)
                                                .build();

                timeSeries.write(timeSeriesRecordSet);
            }

        } finally {

            server.shutdown();
        }

        server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.setQueryTimeoutInSeconds(120);
                Database database = client.getDatabase("test");

                TimeSeries timeSeries = database.getTimeSeries("DAX");

                RecordSet defaultRecordSet = timeSeries.read(TimeUtils.parseDateTime("2013-11-14 00:00:00.000"),
                                                             TimeUtils.parseDateTime("2013-11-14 23:59:59.999"));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(125, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(10, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp + 100, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(120, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(5, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp + 350, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(110, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(10, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testInsertWithForceFlushFromCache() throws Exception {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .memTimeSeriesSize(60)
                                                   .maximumMemoryUsageByMemTimeSeries(100)
                                                   .cachesConcurrencyLevel(1)
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.setQueryTimeoutInSeconds(120);
                Database database = client.newDatabase("test");

                RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                                .addMillisecondTimestampField("timestampInMillis")
                                                                                .addByteField("status")
                                                                                .build();

                TimeSeriesDefinition daxDefinition = database.newTimeSeriesDefinitionBuilder("DAX")
                                                             .timeUnit(TimeUnit.NANOSECONDS)
                                                             .addRecordType(recordTypeDefinition)
                                                             .build();

                TimeSeriesDefinition cacDefinition = database.newTimeSeriesDefinitionBuilder("CAC40")
                                                             .timeUnit(TimeUnit.NANOSECONDS)
                                                             .addRecordType(recordTypeDefinition)
                                                             .build();

                TimeSeries daxTimeSeries = database.createTimeSeries(daxDefinition);

                RecordSet timeSeriesRecordSet = daxTimeSeries.newRecordSetBuilder()
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp)
                                                             .setTimestampInMillis(1, timestamp)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 100)
                                                             .setTimestampInMillis(1, timestamp + 100)
                                                             .setByte(2, 5)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 350)
                                                             .setTimestampInMillis(1, timestamp + 350)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 450)
                                                             .setTimestampInMillis(1, timestamp + 450)
                                                             .setByte(2, 6)
                                                             .build();

                daxTimeSeries.write(timeSeriesRecordSet);

                TimeSeries cacTimeSeries = database.createTimeSeries(cacDefinition);

                timeSeriesRecordSet = cacTimeSeries.newRecordSetBuilder()
                                                   .newRecord("exchangeState")
                                                   .setTimestampInMillis(0, timestamp)
                                                   .setTimestampInMillis(1, timestamp)
                                                   .setByte(2, 10)
                                                   .newRecord("exchangeState")
                                                   .setTimestampInMillis(0, timestamp + 100)
                                                   .setTimestampInMillis(1, timestamp + 100)
                                                   .setByte(2, 5)
                                                   .newRecord("exchangeState")
                                                   .setTimestampInMillis(0, timestamp + 350)
                                                   .setTimestampInMillis(1, timestamp + 350)
                                                   .setByte(2, 10)
                                                   .newRecord("exchangeState")
                                                   .setTimestampInMillis(0, timestamp + 450)
                                                   .setTimestampInMillis(1, timestamp + 450)
                                                   .setByte(2, 6)
                                                   .build();

                cacTimeSeries.write(timeSeriesRecordSet);

                AssertFiles.assertFileExists(this.testDirectory.resolve("data")
                                                               .resolve("test")
                                                               .resolve("DAX-1384383600000.ts"));

                AssertFiles.assertFileDoesNotExists(this.testDirectory.resolve("data")
                                                                      .resolve("test")
                                                                      .resolve("CAC40-1384383600000.ts"));

                RecordSet defaultRecordSet = daxTimeSeries.read(timestamp, timestamp + 20000);

                assertTrue(defaultRecordSet.next());

                assertEquals(timestamp, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(10, defaultRecordSet.getByte(2));

                assertTrue(defaultRecordSet.next());

                assertEquals(timestamp + 100L, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 100L, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(5, defaultRecordSet.getByte(2));

                assertTrue(defaultRecordSet.next());

                assertEquals(timestamp + 350, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 350, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(10, defaultRecordSet.getByte(2));

                assertTrue(defaultRecordSet.next());

                assertEquals(timestamp + 450, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 450, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(6, defaultRecordSet.getByte(2));

                assertFalse(defaultRecordSet.next());

                defaultRecordSet = cacTimeSeries.read(timestamp, timestamp + 20000);

                assertTrue(defaultRecordSet.next());

                assertEquals(timestamp, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(10, defaultRecordSet.getByte(2));

                assertTrue(defaultRecordSet.next());

                assertEquals(timestamp + 100L, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 100L, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(5, defaultRecordSet.getByte(2));

                assertTrue(defaultRecordSet.next());

                assertEquals(timestamp + 350, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 350, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(10, defaultRecordSet.getByte(2));

                assertTrue(defaultRecordSet.next());

                assertEquals(timestamp + 450, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 450, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(6, defaultRecordSet.getByte(2));

                assertFalse(defaultRecordSet.next());
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testInsertWithFlush() throws Exception {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .memTimeSeriesSize(50)
                                                   .maximumMemoryUsageByMemTimeSeries(100)
                                                   .cachesConcurrencyLevel(1)
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.setQueryTimeoutInSeconds(120);
                Database database = client.newDatabase("test");

                RecordTypeDefinition recordTypeDefinition = RecordTypeDefinition.newBuilder("exchangeState")
                                                                                .addMillisecondTimestampField("timestampInMillis")
                                                                                .addByteField("status")
                                                                                .build();

                TimeSeriesDefinition daxDefinition = database.newTimeSeriesDefinitionBuilder("DAX")
                                                             .timeUnit(TimeUnit.NANOSECONDS)
                                                             .addRecordType(recordTypeDefinition)
                                                             .build();

                TimeSeries daxTimeSeries = database.createTimeSeries(daxDefinition);

                RecordSet timeSeriesRecordSet = daxTimeSeries.newRecordSetBuilder()
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp)
                                                             .setTimestampInMillis(1, timestamp)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 100)
                                                             .setTimestampInMillis(1, timestamp + 100)
                                                             .setByte(2, 5)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 350)
                                                             .setTimestampInMillis(1, timestamp + 350)
                                                             .setByte(2, 10)
                                                             .newRecord("exchangeState")
                                                             .setTimestampInMillis(0, timestamp + 450)
                                                             .setTimestampInMillis(1, timestamp + 450)
                                                             .setByte(2, 6)
                                                             .build();

                daxTimeSeries.write(timeSeriesRecordSet);

                Thread.sleep(100);

                AssertFiles.assertFileExists(this.testDirectory.resolve("data")
                                                               .resolve("test")
                                                               .resolve("DAX-1384383600000.ts"));

                RecordSet defaultRecordSet = daxTimeSeries.read(timestamp, timestamp + 20000);

                assertTrue(defaultRecordSet.next());

                assertEquals(timestamp, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(10, defaultRecordSet.getByte(2));

                assertTrue(defaultRecordSet.next());

                assertEquals(timestamp + 100L, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 100L, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(5, defaultRecordSet.getByte(2));

                assertTrue(defaultRecordSet.next());

                assertEquals(timestamp + 350, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 350, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(10, defaultRecordSet.getByte(2));

                assertTrue(defaultRecordSet.next());

                assertEquals(timestamp + 450, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 450, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(6, defaultRecordSet.getByte(2));

                assertFalse(defaultRecordSet.next());
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testReadAcrossMultiplePartitions() throws Exception {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");
        long timestamp2 = TimeUtils.parseDateTime("2013-11-15 08:16:00.000");

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.setQueryTimeoutInSeconds(120);
                Database database = client.newDatabase("test");

                RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("Trade")
                                                                 .addDecimalField("price")
                                                                 .addLongField("volume")
                                                                 .build();

                TimeSeriesDefinition definition = database.newTimeSeriesDefinitionBuilder("DAX")
                                                          .timeUnit(TimeUnit.MILLISECONDS)
                                                          .addRecordType(trade)
                                                          .build();

                TimeSeries timeSeries = database.createTimeSeries(definition);

                RecordSet timeSeriesRecordSet = timeSeries.newRecordSetBuilder()
                                                          .newRecord("Trade")
                                                          .setTimestampInMillis(0, timestamp)
                                                          .setDecimal(1, 125, -1)
                                                          .setLong(2, 10)
                                                          .newRecord("Trade")
                                                          .setTimestampInMillis(0, timestamp + 100)
                                                          .setDecimal(1, 12, 0)
                                                          .setLong(2, 5)
                                                          .newRecord("Trade")
                                                          .setTimestampInMillis(0, timestamp + 350)
                                                          .setDecimal(1, 11, 0)
                                                          .setLong(2, 10)
                                                          .build();

                timeSeries.write(timeSeriesRecordSet);

                timeSeriesRecordSet = timeSeries.newRecordSetBuilder()
                                                .newRecord("Trade")
                                                .setTimestampInMillis(0, timestamp2)
                                                .setDecimal(1, 13, 0)
                                                .setLong(2, 5)
                                                .newRecord("Trade")
                                                .setTimestampInMillis(0, timestamp2 + 150)
                                                .setDecimal(1, 129, -1)
                                                .setLong(2, 5)
                                                .newRecord("Trade")
                                                .setTimestampInMillis(0, timestamp2 + 350)
                                                .setDecimal(1, 13, 0)
                                                .setLong(2, 10)
                                                .build();

                timeSeries.write(timeSeriesRecordSet);

                RecordSet defaultRecordSet = timeSeries.read(TimeUtils.parseDateTime("2013-11-14 00:00:00.000"),
                                                             TimeUtils.parseDateTime("2013-11-15 23:59:59.999"));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(125, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(10, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp + 100, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(120, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(5, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp + 350, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(110, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(10, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp2, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(13, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(0, defaultRecordSet.getDecimalExponent(1));
                assertEquals(5, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp2 + 150, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(129, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(5, defaultRecordSet.getLong(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp2 + 350, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(130, defaultRecordSet.getDecimalMantissa(1));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(1));
                assertEquals(10, defaultRecordSet.getLong(2));
                
                assertFalse(defaultRecordSet.next());
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testReadWithNoData() throws Exception {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.setQueryTimeoutInSeconds(120);
                Database database = client.newDatabase("test");

                RecordTypeDefinition exchangeState = RecordTypeDefinition.newBuilder("exchangeState")
                                                                         .addMillisecondTimestampField("timestampInMillis")
                                                                         .addByteField("status")
                                                                         .build();

                RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("trade")
                                                                 .addMillisecondTimestampField("timestampInMillis")
                                                                 .addDecimalField("price")
                                                                 .addLongField("volume")
                                                                 .build();

                TimeSeriesDefinition definition = database.newTimeSeriesDefinitionBuilder("DAX")
                                                          .timeUnit(TimeUnit.MILLISECONDS)
                                                          .addRecordType(exchangeState)
                                                          .addRecordType(trade)
                                                          .build();

                TimeSeries timeSeries = database.createTimeSeries(definition);

                RecordSet defaultRecordSet = timeSeries.read(timestamp + 200, timestamp + 400);

                assertFalse(defaultRecordSet.next());
            }

        } finally {

            server.shutdown();
        }
    }
    
    @Test
    public void testReadWithFiltering() throws Exception {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonClient client = new HorizonClient(configuration.getPort())) {

                client.setQueryTimeoutInSeconds(120);
                Database database = client.newDatabase("test");

                RecordTypeDefinition exchangeState = RecordTypeDefinition.newBuilder("exchangeState")
                                                                         .addMillisecondTimestampField("timestampInMillis")
                                                                         .addByteField("status")
                                                                         .build();

                RecordTypeDefinition trade = RecordTypeDefinition.newBuilder("trade")
                                                                 .addMillisecondTimestampField("timestampInMillis")
                                                                 .addDecimalField("price")
                                                                 .addLongField("volume")
                                                                 .build();

                TimeSeriesDefinition definition = database.newTimeSeriesDefinitionBuilder("DAX")
                                                          .timeUnit(TimeUnit.MILLISECONDS)
                                                          .addRecordType(exchangeState)
                                                          .addRecordType(trade)
                                                          .build();

                TimeSeries timeSeries = database.createTimeSeries(definition);

                RecordSet timeSeriesRecordSet = timeSeries.newRecordSetBuilder()
                                                          .newRecord("exchangeState")
                                                          .setTimestampInMillis(0, timestamp)
                                                          .setTimestampInMillis(1, timestamp)
                                                          .setByte(2, 10)
                                                          .newRecord("trade")
                                                          .setTimestampInMillis(0, timestamp)
                                                          .setTimestampInMillis(1, timestamp)
                                                          .setDecimal(2, 12, 0)
                                                          .setLong(3, 6)
                                                          .newRecord("exchangeState")
                                                          .setTimestampInMillis(0, timestamp + 100)
                                                          .setTimestampInMillis(1, timestamp + 100)
                                                          .setByte(2, 5)
                                                          .newRecord("exchangeState")
                                                          .setTimestampInMillis(0, timestamp + 350)
                                                          .setTimestampInMillis(1, timestamp + 350)
                                                          .setByte(2, 10)
                                                          .newRecord("trade")
                                                          .setTimestampInMillis(0, timestamp + 360)
                                                          .setTimestampInMillis(1, timestamp + 360)
                                                          .setDecimal(2, 125, -1)
                                                          .setLong(3, 4)
                                                          .newRecord("exchangeState")
                                                          .setTimestampInMillis(0, timestamp + 450)
                                                          .setTimestampInMillis(1, timestamp + 450)
                                                          .setByte(2, 6)
                                                          .newRecord("trade")
                                                          .setTimestampInMillis(0, timestamp + 500)
                                                          .setTimestampInMillis(1, timestamp + 500)
                                                          .setDecimal(2, 13, 0)
                                                          .setLong(3, 9)
                                                          .build();

                timeSeries.write(timeSeriesRecordSet);

                RecordSet defaultRecordSet = timeSeries.read(timestamp + 200, timestamp + 400);

                assertTrue(defaultRecordSet.next());
                assertEquals(0, defaultRecordSet.getType());
                assertEquals(timestamp + 350, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 350, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(10, defaultRecordSet.getByte(2));

                assertTrue(defaultRecordSet.next());
                assertEquals(timestamp + 360, defaultRecordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 360, defaultRecordSet.getTimestampInMillis(1));
                assertEquals(125, defaultRecordSet.getDecimalMantissa(2));
                assertEquals(-1, defaultRecordSet.getDecimalExponent(2));
                assertEquals(4, defaultRecordSet.getLong(3));

                assertFalse(defaultRecordSet.next());
            }

        } finally {

            server.shutdown();
        }
    }
}
