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
import io.horizondb.test.AssertFiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
public class HorizonDBTest {

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

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).build()) {

                client.newConnection("test");
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

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).build()) {

                client.newConnection().execute("CREATE DATABASE Test;");
            }

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).build()) {

                Connection connection = client.newConnection("test");
                Assert.assertEquals("Test", connection.getDatabase());
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

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).setQueryTimeoutInSeconds(3).build()) {

                client.newConnection(" ");
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

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).build()) {

                Connection connection = client.newConnection();
                
                connection.execute("CREATE DATABASE Test;");
                connection.execute("USE Test;");
                
                Assert.assertEquals("Test", connection.getDatabase());
                
                connection.execute("CREATE TIMESERIES DAX (" +
                                        "Quote(received NANOSECONDS_TIMESTAMP, bidPrice DECIMAL, askPrice DECIMAL, bidVolume INTEGER, askVolume INTEGER), " +
                                        "Trade(received NANOSECONDS_TIMESTAMP, price DECIMAL, volume INTEGER))TIME_UNIT = NANOSECONDS TIMEZONE = 'Europe/Berlin';");
              }

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).build()) {

                Connection connection = client.newConnection("Test");
                
                RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '26-05-2014' AND '27-05-2014';");
            
                assertFalse(recordSet.next());
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

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).build()) {

                Connection connection = client.newConnection();
                connection.execute("CREATE DATABASE Test;");
            }

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).build()) {

                Connection connection = client.newConnection("Test");
                connection.execute("SELECT * FROM DAX30 WHERE timestamp BETWEEN '26-05-2014' AND '27-05-2014';");

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

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).setQueryTimeoutInSeconds(120).build()) {

                Connection connection = client.newConnection();
                
                connection.execute("CREATE DATABASE test;");
                connection.execute("USE test;");
                
                Assert.assertEquals("test", connection.getDatabase());
                
                connection.execute("CREATE TIMESERIES DAX (" +
                                        "Trade(price DECIMAL, volume INTEGER))TIME_UNIT = MILLISECONDS TIMEZONE = 'Europe/Berlin';");
                
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.000', 125E-1, 10);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.100', 12, 5);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.350', 11, 10);");

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '2013-11-14 11:46:00' AND '2013-11-14 11:46:02';")) {

                    assertTrue(recordSet.next());
                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(125, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(10, recordSet.getLong(2));
    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 100, recordSet.getTimestampInMillis(0));
                    assertEquals(120, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(5, recordSet.getLong(2));
                }
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

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).setQueryTimeoutInSeconds(120).build()) {

                Connection connection = client.newConnection();
                
                connection.execute("CREATE DATABASE test;");
                connection.execute("USE test;");
                
                Assert.assertEquals("test", connection.getDatabase());
                
                connection.execute("CREATE TIMESERIES DAX (" +
                                        "Trade(price DECIMAL, volume INTEGER))TIME_UNIT = MILLISECONDS TIMEZONE = 'Europe/Berlin';");
                
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.000', 125E-1, 10);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.100', 12, 5);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.350', 11, 10);");

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '2013-11-14 11:46:00' AND '2013-11-14 11:46:02';")) {

                    assertTrue(recordSet.next());
                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(125, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(10, recordSet.getLong(2));
    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 100, recordSet.getTimestampInMillis(0));
                    assertEquals(120, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(5, recordSet.getLong(2));

                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 350, recordSet.getTimestampInMillis(0));
                    assertEquals(110, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(10, recordSet.getLong(2));

                    assertFalse(recordSet.next());
                }
            }     

        } finally {

            server.shutdown();
        }

        AssertFiles.assertFileExists(this.testDirectory.resolve("data").resolve("test").resolve("DAX-1384383600000.ts"));

        server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).setQueryTimeoutInSeconds(120).build()) {

                Connection connection = client.newConnection("test");

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '2013-11-14 11:46:00' AND '2013-11-14 11:46:02';")) {

                    assertTrue(recordSet.next());
                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(125, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(10, recordSet.getLong(2));
    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 100, recordSet.getTimestampInMillis(0));
                    assertEquals(120, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(5, recordSet.getLong(2));

                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 350, recordSet.getTimestampInMillis(0));
                    assertEquals(110, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(10, recordSet.getLong(2));

                    assertFalse(recordSet.next());
            
                }
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

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).setQueryTimeoutInSeconds(120).build()) {

                Connection connection = client.newConnection();
                
                connection.execute("CREATE DATABASE test;");
                connection.execute("USE test;");
                
                Assert.assertEquals("test", connection.getDatabase());
                
                connection.execute("CREATE TIMESERIES DAX (" +
                                        "Trade(price DECIMAL, volume INTEGER))TIME_UNIT = MILLISECONDS TIMEZONE = 'Europe/Berlin';");
                
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.000', 125E-1, 10);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.100', 12, 5);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.350', 11, 10);");
                
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.400', 125E-1, 10);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.500', 12, 5);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.650', 11, 10);");
                
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.800', 125E-1, 10);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.850', 12, 5);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.900', 11, 10);");
                
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:01.000', 125E-1, 10);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:01.050', 12, 5);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:01.200', 11, 10);");
            }

        } finally {

            server.shutdown();
        }

        server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).setQueryTimeoutInSeconds(120).build()) {

                Connection connection = client.newConnection("test");

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '2013-11-14' AND '2013-11-15';")) {

                    assertTrue(recordSet.next());
                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(125, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(10, recordSet.getLong(2));
    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 100, recordSet.getTimestampInMillis(0));
                    assertEquals(120, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(5, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 350, recordSet.getTimestampInMillis(0));
                    assertEquals(110, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(10, recordSet.getLong(2));

                    assertTrue(recordSet.next());
                }
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

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).setQueryTimeoutInSeconds(120).build()) {

                Connection connection = client.newConnection();
                
                connection.execute("CREATE DATABASE test;");
                connection.execute("USE test;");
                
                Assert.assertEquals("test", connection.getDatabase());
                
                connection.execute("CREATE TIMESERIES DAX (" +
                                        "ExchangeState(timestampInMillis MILLISECONDS_TIMESTAMP, status BYTE))TIME_UNIT = NANOSECONDS TIMEZONE = 'Europe/Berlin';");
                
                connection.execute("INSERT INTO DAX.ExchangeState VALUES ('2013-11-14 11:46:00.000', '2013-11-14 11:46:00.000', 10);");
                connection.execute("INSERT INTO DAX.ExchangeState VALUES ('2013-11-14 11:46:00.100', '2013-11-14 11:46:00.100', 5);");
                connection.execute("INSERT INTO DAX.ExchangeState VALUES ('2013-11-14 11:46:00.350', '2013-11-14 11:46:00.350', 10);");
                connection.execute("INSERT INTO DAX.ExchangeState VALUES ('2013-11-14 11:46:00.450', '2013-11-14 11:46:00.450', 6);");
                
                connection.execute("CREATE TIMESERIES CAC40 (" +
                        "ExchangeState(timestampInMillis MILLISECONDS_TIMESTAMP, status BYTE))TIME_UNIT = NANOSECONDS TIMEZONE = 'Europe/Berlin';");

                connection.execute("INSERT INTO CAC40.ExchangeState VALUES ('2013-11-14 11:46:00.000', '2013-11-14 11:46:00.000', 10);");
                connection.execute("INSERT INTO CAC40.ExchangeState VALUES ('2013-11-14 11:46:00.100', '2013-11-14 11:46:00.100', 5);");
                connection.execute("INSERT INTO CAC40.ExchangeState VALUES ('2013-11-14 11:46:00.350', '2013-11-14 11:46:00.350', 10);");
                connection.execute("INSERT INTO CAC40.ExchangeState VALUES ('2013-11-14 11:46:00.450', '2013-11-14 11:46:00.450', 6);");

                AssertFiles.assertFileExists(this.testDirectory.resolve("data")
                                                               .resolve("test")
                                                               .resolve("DAX-1384383600000.ts"));

                AssertFiles.assertFileDoesNotExists(this.testDirectory.resolve("data")
                                                                      .resolve("test")
                                                                      .resolve("CAC40-1384383600000.ts"));

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '2013-11-14' AND '2013-11-15';")) {

                    assertTrue(recordSet.next());

                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp, recordSet.getTimestampInMillis(1));
                    assertEquals(10, recordSet.getByte(2));

                    assertTrue(recordSet.next());

                    assertEquals(timestamp + 100L, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 100L, recordSet.getTimestampInMillis(1));
                    assertEquals(5, recordSet.getByte(2));

                    assertTrue(recordSet.next());

                    assertEquals(timestamp + 350, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 350, recordSet.getTimestampInMillis(1));
                    assertEquals(10, recordSet.getByte(2));

                    assertTrue(recordSet.next());

                    assertEquals(timestamp + 450, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 450, recordSet.getTimestampInMillis(1));
                    assertEquals(6, recordSet.getByte(2));

                    assertFalse(recordSet.next());

                }

                try (RecordSet recordSet = connection.execute("SELECT * FROM CAC40 WHERE timestamp BETWEEN '2013-11-14' AND '2013-11-15';")) {

                    assertTrue(recordSet.next());

                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp, recordSet.getTimestampInMillis(1));
                    assertEquals(10, recordSet.getByte(2));

                    assertTrue(recordSet.next());

                    assertEquals(timestamp + 100L, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 100L, recordSet.getTimestampInMillis(1));
                    assertEquals(5, recordSet.getByte(2));

                    assertTrue(recordSet.next());

                    assertEquals(timestamp + 350, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 350, recordSet.getTimestampInMillis(1));
                    assertEquals(10, recordSet.getByte(2));

                    assertTrue(recordSet.next());

                    assertEquals(timestamp + 450, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 450, recordSet.getTimestampInMillis(1));
                    assertEquals(6, recordSet.getByte(2));

                    assertFalse(recordSet.next());

                }
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

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).setQueryTimeoutInSeconds(120).build()) {

                Connection connection = client.newConnection();
                
                connection.execute("CREATE DATABASE test;");
                connection.execute("USE test;");
                
                Assert.assertEquals("test", connection.getDatabase());
                
                connection.execute("CREATE TIMESERIES DAX (" +
                                        "ExchangeState(timestampInMillis MILLISECONDS_TIMESTAMP, status BYTE))TIME_UNIT = NANOSECONDS TIMEZONE = 'Europe/Berlin';");
                
                connection.execute("INSERT INTO DAX.ExchangeState VALUES ('2013-11-14 11:46:00.000', '2013-11-14 11:46:00.000', 10);");
                connection.execute("INSERT INTO DAX.ExchangeState VALUES ('2013-11-14 11:46:00.100', '2013-11-14 11:46:00.100', 5);");
                connection.execute("INSERT INTO DAX.ExchangeState VALUES ('2013-11-14 11:46:00.350', '2013-11-14 11:46:00.350', 10);");
                connection.execute("INSERT INTO DAX.ExchangeState VALUES ('2013-11-14 11:46:00.450', '2013-11-14 11:46:00.450', 6);");

                Thread.sleep(100);
                
                AssertFiles.assertFileExists(this.testDirectory.resolve("data")
                                                               .resolve("test")
                                                               .resolve("DAX-1384383600000.ts"));

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '2013-11-14' AND '2013-11-15';")) {

                    assertTrue(recordSet.next());

                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp, recordSet.getTimestampInMillis(1));
                    assertEquals(10, recordSet.getByte(2));

                    assertTrue(recordSet.next());

                    assertEquals(timestamp + 100L, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 100L, recordSet.getTimestampInMillis(1));
                    assertEquals(5, recordSet.getByte(2));

                    assertTrue(recordSet.next());

                    assertEquals(timestamp + 350, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 350, recordSet.getTimestampInMillis(1));
                    assertEquals(10, recordSet.getByte(2));

                    assertTrue(recordSet.next());

                    assertEquals(timestamp + 450, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 450, recordSet.getTimestampInMillis(1));
                    assertEquals(6, recordSet.getByte(2));

                    assertFalse(recordSet.next());
                }
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

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).setQueryTimeoutInSeconds(120).build()) {

                Connection connection = client.newConnection();
                
                connection.execute("CREATE DATABASE test;");
                connection.execute("USE test;");
                
                Assert.assertEquals("test", connection.getDatabase());
                
                connection.execute("CREATE TIMESERIES DAX (" +
                                        "Trade(price DECIMAL, volume INTEGER))TIME_UNIT = MILLISECONDS TIMEZONE = 'Europe/Berlin';");
                
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.000', 125E-1, 10);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.100', 12, 5);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.350', 11, 10);");

                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-15 08:16:00.000', 13, 5);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-15 08:16:00.150', 129E-1, 5);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-15 08:16:00.350', 13, 10);");

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '2013-11-14' AND '2013-11-16';")) {

                    assertTrue(recordSet.next());
                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(125, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(10, recordSet.getLong(2));

                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 100, recordSet.getTimestampInMillis(0));
                    assertEquals(120, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(5, recordSet.getLong(2));

                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 350, recordSet.getTimestampInMillis(0));
                    assertEquals(110, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(10, recordSet.getLong(2));

                    assertTrue(recordSet.next());
                    assertEquals(timestamp2, recordSet.getTimestampInMillis(0));
                    assertEquals(13, recordSet.getDecimalMantissa(1));
                    assertEquals(0, recordSet.getDecimalExponent(1));
                    assertEquals(5, recordSet.getLong(2));

                    assertTrue(recordSet.next());
                    assertEquals(timestamp2 + 150, recordSet.getTimestampInMillis(0));
                    assertEquals(129, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(5, recordSet.getLong(2));

                    assertTrue(recordSet.next());
                    assertEquals(timestamp2 + 350, recordSet.getTimestampInMillis(0));
                    assertEquals(130, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(10, recordSet.getLong(2));

                    assertFalse(recordSet.next());
                }
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testReadWithNoData() throws Exception {

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).setQueryTimeoutInSeconds(120).build()) {

                Connection connection = client.newConnection();
                
                connection.execute("CREATE DATABASE test;");
                connection.execute("USE test;");
                
                Assert.assertEquals("test", connection.getDatabase());
                
                connection.execute("CREATE TIMESERIES DAX (" +
                                        "ExchangeState(timestampInMillis MILLISECONDS_TIMESTAMP, status BYTE)," +
                                        "Trade(price DECIMAL, volume INTEGER))TIME_UNIT = MILLISECONDS TIMEZONE = 'Europe/Berlin';");

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '2013-11-14' AND '2013-11-16';")) {

                    assertFalse(recordSet.next());
                }
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

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).setQueryTimeoutInSeconds(120).build()) {

                Connection connection = client.newConnection();
                
                connection.execute("CREATE DATABASE test;");
                connection.execute("USE test;");
                
                Assert.assertEquals("test", connection.getDatabase());
                
                connection.execute("CREATE TIMESERIES DAX (" +
                                        " ExchangeState(timestampInMillis MILLISECONDS_TIMESTAMP, status BYTE), " +
                                        " Trade(timestampInMillis MILLISECONDS_TIMESTAMP, price DECIMAL, volume INTEGER)) TIME_UNIT = MILLISECONDS TIMEZONE = 'Europe/Berlin';");

                connection.execute("INSERT INTO DAX.ExchangeState VALUES ('2013-11-14 11:46:00.000', '2013-11-14 11:46:00.000', 10);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.000', '2013-11-14 11:46:00.000', 12, 6);");
                connection.execute("INSERT INTO DAX.ExchangeState VALUES ('2013-11-14 11:46:00.100', '2013-11-14 11:46:00.100', 5);");
                connection.execute("INSERT INTO DAX.ExchangeState VALUES ('2013-11-14 11:46:00.350', '2013-11-14 11:46:00.350', 10);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.360', '2013-11-14 11:46:00.360', 125E-1, 4);");
                connection.execute("INSERT INTO DAX.ExchangeState VALUES ('2013-11-14 11:46:00.450', '2013-11-14 11:46:00.450', 6);");
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.500', '2013-11-14 11:46:00.500', 13, 9);");

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN " + (timestamp + 200) + "ms AND " + (timestamp + 400) + "ms;")) {

                assertTrue(recordSet.next());
                assertEquals(0, recordSet.getType());
                assertEquals(timestamp + 350, recordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 350, recordSet.getTimestampInMillis(1));
                assertEquals(10, recordSet.getByte(2));

                assertTrue(recordSet.next());
                assertEquals(timestamp + 360, recordSet.getTimestampInMillis(0));
                assertEquals(timestamp + 360, recordSet.getTimestampInMillis(1));
                assertEquals(125, recordSet.getDecimalMantissa(2));
                assertEquals(-1, recordSet.getDecimalExponent(2));
                assertEquals(4, recordSet.getLong(3));

                assertFalse(recordSet.next());
                }
            }

        } finally {

            server.shutdown();
        }
    }
}