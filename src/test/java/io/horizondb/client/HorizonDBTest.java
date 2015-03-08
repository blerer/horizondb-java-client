/**
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
import io.horizondb.db.commitlog.CommitLog.SyncMode;
import io.horizondb.io.files.FileUtils;
import io.horizondb.model.ErrorCodes;
import io.horizondb.model.core.util.TimeUtils;
import io.horizondb.model.schema.RecordSetDefinition;
import io.horizondb.test.AssertFiles;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.commons.lang.Validate.notEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
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
    public void testUseDatabaseWithNonExistingDatabase() throws Exception {

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).build()) {

                Connection connection = client.newConnection();
                
                connection.execute("USE Test;");
            
            } catch(HorizonDBException e) { 
                assertEquals(ErrorCodes.UNKNOWN_DATABASE, e.getCode());
                assertTrue(e.getMessage().contains("The database 'test' does not exists."));
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
                
                RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '2014-05-26' AND '2014-05-26';");
            
                assertFalse(recordSet.next());
            }

        } finally {

            server.shutdown();
        }
    }
    
    @Test
    public void testCreateTimeSeriesWithDatabaseNameSpecifiedInTheQuery() throws Exception {

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
                
                connection.execute("CREATE TIMESERIES Test.DAX (" +
                                        "Quote(received NANOSECONDS_TIMESTAMP, bidPrice DECIMAL, askPrice DECIMAL, bidVolume INTEGER, askVolume INTEGER), " +
                                        "Trade(received NANOSECONDS_TIMESTAMP, price DECIMAL, volume INTEGER))TIME_UNIT = NANOSECONDS TIMEZONE = 'Europe/Berlin';");
              }

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).build()) {

                Connection connection = client.newConnection("Test");
                
                RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '2014-05-26' AND '2014-05-26';");
            
                assertFalse(recordSet.next());
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testCreateTimeSeriesWithNoDatabaseSpecified() throws Exception {

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).build()) {

                Connection connection = client.newConnection();

                connection.execute("CREATE TIMESERIES DAX (" +
                                        "Quote(received NANOSECONDS_TIMESTAMP, bidPrice DECIMAL, askPrice DECIMAL, bidVolume INTEGER, askVolume INTEGER), " +
                                        "Trade(received NANOSECONDS_TIMESTAMP, price DECIMAL, volume INTEGER))TIME_UNIT = NANOSECONDS TIMEZONE = 'Europe/Berlin';");
            
            } catch(HorizonDBException e) { 
                assertError(ErrorCodes.UNKNOWN_DATABASE, "No database has been specified.", e);
            }

        } finally {

            server.shutdown();
        }
    }    
    
    @Test
    public void testCreateTimeSeriesWithInvalidTypeName() throws Exception {

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

                connection.execute("CREATE TIMESERIES DAX ("
                        + "Quote(received NANOSECONDS_TIMESTAMP, bidPrice DCIMAL, askPrice DECIMAL, bidVolume INTEGER, askVolume INTEGER), "
                        + "Trade(received NANOSECONDS_TIMESTAMP, price DECIMAL, volume INTEGER))TIME_UNIT = NANOSECONDS TIMEZONE = 'Europe/Berlin';");

                fail();

            } catch (HorizonDBException e) {
                assertError(ErrorCodes.INVALID_QUERY, "mismatched input 'DCIMAL'", e);
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testSelectWithNotExistingSeries() throws Exception {

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

                fail();

            } catch (HorizonDBException e) {

                assertError(ErrorCodes.UNKNOWN_TIMESERIES, "The time series dax30 does not exists within the database test.", e);
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
    public void testDropTimeSeries() throws Exception {

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
                
                connection.execute("DROP TIMESERIES DAX;");
                
                connection.execute("CREATE TIMESERIES DAX (" +
                        "Trade(price DECIMAL, volume INTEGER))TIME_UNIT = MILLISECONDS TIMEZONE = 'Europe/Berlin';");
                
                connection.execute("INSERT INTO DAX.Trade VALUES ('2013-11-14 11:46:00.100', 13, 6);");

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '2013-11-14 11:46:00' AND '2013-11-14 11:46:02';")) {

                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 100, recordSet.getTimestampInMillis(0));
                    assertEquals(13.0, recordSet.getDouble(1), 0);
                    assertEquals(6, recordSet.getLong(2));
                }
            }

        } finally {

            server.shutdown();
        }
    }
    
    @Test
    public void testInsertIntoTimeSeriesWithInvalidValues() throws Exception {

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
                
                connection.execute("INSERT INTO DAX.Trade VALUES ('test', 125E-1, 10);");
                fail();

            } catch (HorizonDBException e) {

                assertError(ErrorCodes.INVALID_QUERY, "The format of the date/time: test does not match the expected one: yyyy-MM-dd HH:mm:ss.SSS", e);
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

        assertTimeSeriesFileExist("test", "DAX", 1384383600000L);

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
    public void testInsertIntoTimeSeriesWithWithCommitLogSegmentSwitchAndForceFlushInBatchMode() 
            throws Exception {
        

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .commitLogSegmentSize(200)
                                                   .maximumNumberOfCommitLogSegments(3)
                                                   .build();
        
        testInsertIntoTimeSeriesWithWithCommitLogSegmentSwitchAndForceFlush(configuration); 
    }

    @Test
    public void testInsertIntoTimeSeriesWithWithCommitLogSegmentSwitchAndForceFlushInPeriodicMode() 
            throws Exception {
        

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .commitLogSegmentSize(200)
                                                   .maximumNumberOfCommitLogSegments(3)
                                                   .commitLogSyncMode(SyncMode.PERIODIC)
                                                   .commitLogFlushPeriodInMillis(50)
                                                   .build();
        
        testInsertIntoTimeSeriesWithWithCommitLogSegmentSwitchAndForceFlush(configuration); 
    }
    
    private static void testInsertIntoTimeSeriesWithWithCommitLogSegmentSwitchAndForceFlush(Configuration configuration) 
            throws Exception {

        long timestamp = TimeUtils.parseDateTime("2013-11-14 11:46:00.000");

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
                
                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN '2013-11-14' AND '2013-11-15';")) {

                    assertTrue(recordSet.next());
                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(12.5, recordSet.getDouble(1), 0);
                    assertEquals(10, recordSet.getLong(2));
    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 100, recordSet.getTimestampInMillis(0));
                    assertEquals(12, recordSet.getDouble(1), 0);
                    assertEquals(5, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 350, recordSet.getTimestampInMillis(0));
                    assertEquals(11, recordSet.getDouble(1), 0);
                    assertEquals(10, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 400, recordSet.getTimestampInMillis(0));
                    assertEquals(12.5, recordSet.getDouble(1), 0.0);
                    assertEquals(10, recordSet.getLong(2));
    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 500, recordSet.getTimestampInMillis(0));
                    assertEquals(12.0, recordSet.getDouble(1), 0.0);
                    assertEquals(5, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 650, recordSet.getTimestampInMillis(0));
                    assertEquals(11, recordSet.getDouble(1), 0.0);
                    assertEquals(10, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 800, recordSet.getTimestampInMillis(0));
                    assertEquals(12.5, recordSet.getDouble(1), 0.0);
                    assertEquals(10, recordSet.getLong(2));
    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 850, recordSet.getTimestampInMillis(0));
                    assertEquals(12.0, recordSet.getDouble(1), 0.0);
                    assertEquals(5, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 900, recordSet.getTimestampInMillis(0));
                    assertEquals(11, recordSet.getDouble(1), 0);
                    assertEquals(10, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 1000, recordSet.getTimestampInMillis(0));
                    assertEquals(12.5, recordSet.getDouble(1), 0);
                    assertEquals(10, recordSet.getLong(2));
    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 1050, recordSet.getTimestampInMillis(0));
                    assertEquals(12.0, recordSet.getDouble(1), 0.0);
                    assertEquals(5, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 1200, recordSet.getTimestampInMillis(0));
                    assertEquals(11.0, recordSet.getDouble(1), 0.0);
                    assertEquals(10, recordSet.getLong(2));

                    assertFalse(recordSet.next());
                }
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
                    assertEquals(12, recordSet.getDouble(1), 0);
                    assertEquals(5, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 350, recordSet.getTimestampInMillis(0));
                    assertEquals(110, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(10, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 400, recordSet.getTimestampInMillis(0));
                    assertEquals(125, recordSet.getDecimalMantissa(1));
                    assertEquals(-1, recordSet.getDecimalExponent(1));
                    assertEquals(10, recordSet.getLong(2));
    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 500, recordSet.getTimestampInMillis(0));
                    assertEquals(12, recordSet.getDouble(1), 0);
                    assertEquals(5, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 650, recordSet.getTimestampInMillis(0));
                    assertEquals(11, recordSet.getDouble(1), 0.0);
                    assertEquals(10, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 800, recordSet.getTimestampInMillis(0));
                    assertEquals(12.5, recordSet.getDouble(1), 0.0);
                    assertEquals(10, recordSet.getLong(2));
    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 850, recordSet.getTimestampInMillis(0));
                    assertEquals(12.0, recordSet.getDouble(1), 0.0);
                    assertEquals(5, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 900, recordSet.getTimestampInMillis(0));
                    assertEquals(11, recordSet.getDouble(1), 0);
                    assertEquals(10, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 1000, recordSet.getTimestampInMillis(0));
                    assertEquals(12.5, recordSet.getDouble(1), 0);
                    assertEquals(10, recordSet.getLong(2));
    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 1050, recordSet.getTimestampInMillis(0));
                    assertEquals(12.0, recordSet.getDouble(1), 0.0);
                    assertEquals(5, recordSet.getLong(2));
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 1200, recordSet.getTimestampInMillis(0));
                    assertEquals(11.0, recordSet.getDouble(1), 0.0);
                    assertEquals(10, recordSet.getLong(2));

                    assertFalse(recordSet.next());
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
                                                   .memTimeSeriesSize(70)
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

                assertTimeSeriesFileExist("test", "DAX", 1384383600000L);

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

                assertTimeSeriesFileExist("test", "DAX", 1384383600000L);

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
    public void testReadAcrossMultiplePartitionsWithNoTimestampRestriction() throws Exception {

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

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX;")) {

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
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestamp BETWEEN "
                        + (timestamp + 200) + "ms AND " + (timestamp + 400) + "ms;")) {

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

    @Test
    public void testReadWithProjection() throws Exception {

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
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT Trade.* FROM DAX WHERE timestamp BETWEEN "
                        + (timestamp + 200) + "ms AND " + (timestamp + 400) + "ms;")) {

                    RecordSetDefinition definition = recordSet.getRecordSetDefinition();
                    assertEquals(1, definition.getNumberOfRecordTypes());
                    
                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 360, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 360, recordSet.getTimestampInMillis(1));
                    assertEquals(12.5, recordSet.getDouble(2), 0.0);
                    assertEquals(4, recordSet.getLong(3));

                    assertFalse(recordSet.next());
                }
            }

        } finally {

            server.shutdown();
        }
    }
    
    @Test
    public void testReadWithFieldAndRecordFiltering() throws Exception {

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
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT Trade.timestamp, Trade.price FROM DAX WHERE timestamp BETWEEN "
                        + (timestamp + 200) + "ms AND " + (timestamp + 400) + "ms;")) {

                    assertTrue(recordSet.next());
                    assertEquals(timestamp + 360, recordSet.getTimestampInMillis(0));
                    assertEquals(12.5, recordSet.getDouble(1), 0.0);

                    assertFalse(recordSet.next());
                }
            }

        } finally {

            server.shutdown();
        }
    }
    
    @Test
    public void testReadWithEqualsPredicate() throws Exception {

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
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE volume = 6;")) {

                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp, recordSet.getTimestampInMillis(1));
                    assertEquals(12, recordSet.getDecimalMantissa(2));
                    assertEquals(0, recordSet.getDecimalExponent(2));
                    assertEquals(6, recordSet.getLong(3));

                    assertFalse(recordSet.next());
                }
            }

        } finally {

            server.shutdown();
        }
    }

    @Test
    public void testReadWithEqualsPredicateAndTwoDifferentRecordTypes() throws Exception {

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
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE timestampInMillis = " + timestamp + "ms;")) {

                    assertTrue(recordSet.next());
                    assertEquals(0, recordSet.getType());
                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp, recordSet.getTimestampInMillis(1));
                    assertEquals(10, recordSet.getByte(2));

                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp, recordSet.getTimestampInMillis(1));
                    assertEquals(12, recordSet.getDecimalMantissa(2));
                    assertEquals(0, recordSet.getDecimalExponent(2));
                    assertEquals(6, recordSet.getLong(3));

                    assertFalse(recordSet.next());
                }
            }

        } finally {

            server.shutdown();
        }
    }
    
    @Test
    public void testReadWithGreaterThanPredicate() throws Exception {

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
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE volume > 6;")) {

                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
                    assertEquals(timestamp + 500, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 500, recordSet.getTimestampInMillis(1));
                    assertEquals(130, recordSet.getDecimalMantissa(2));
                    assertEquals(-1, recordSet.getDecimalExponent(2));
                    assertEquals(9, recordSet.getLong(3));
                    
                    assertFalse(recordSet.next());
                }
            }

        } finally {

            server.shutdown();
        }
    }
    
    @Test
    public void testReadWithGreaterOrEqualsPredicate() throws Exception {

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
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE volume >= 6;")) {

                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp, recordSet.getTimestampInMillis(1));
                    assertEquals(12, recordSet.getDecimalMantissa(2));
                    assertEquals(0, recordSet.getDecimalExponent(2));
                    assertEquals(6, recordSet.getLong(3));
                    
                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
                    assertEquals(timestamp + 500, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 500, recordSet.getTimestampInMillis(1));
                    assertEquals(130, recordSet.getDecimalMantissa(2));
                    assertEquals(-1, recordSet.getDecimalExponent(2));
                    assertEquals(9, recordSet.getLong(3));
                    
                    assertFalse(recordSet.next());
                }
            }

        } finally {

            server.shutdown();
        }
    }
    
    @Test
    public void testReadWithLessOrEqualsPredicate() throws Exception {

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
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE volume <= 6;")) {

                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
                    assertEquals(timestamp, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp, recordSet.getTimestampInMillis(1));
                    assertEquals(12, recordSet.getDecimalMantissa(2));
                    assertEquals(0, recordSet.getDecimalExponent(2));
                    assertEquals(6, recordSet.getLong(3));
                    
                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
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
    
    @Test
    public void testReadWithLessThanEqualsPredicate() throws Exception {

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
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE volume < 6;")) {
                    
                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
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
    
    @Test
    public void testReadWithOrPredicate() throws Exception {

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
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE volume < 6 OR volume > 7 ;")) {

                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
                    assertEquals(timestamp + 360, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 360, recordSet.getTimestampInMillis(1));
                    assertEquals(125, recordSet.getDecimalMantissa(2));
                    assertEquals(-1, recordSet.getDecimalExponent(2));
                    assertEquals(4, recordSet.getLong(3));
                    
                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
                    assertEquals(timestamp + 500, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 500, recordSet.getTimestampInMillis(1));
                    assertEquals(130, recordSet.getDecimalMantissa(2));
                    assertEquals(-1, recordSet.getDecimalExponent(2));
                    assertEquals(9, recordSet.getLong(3));
                    
                    assertFalse(recordSet.next());
                }
            }

        } finally {

            server.shutdown();
        }
    }
        
    @Test
    public void testReadWithInPredicate() throws Exception {

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
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE volume IN (4, 7, 9);")) {

                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
                    assertEquals(timestamp + 360, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 360, recordSet.getTimestampInMillis(1));
                    assertEquals(125, recordSet.getDecimalMantissa(2));
                    assertEquals(-1, recordSet.getDecimalExponent(2));
                    assertEquals(4, recordSet.getLong(3));
                    
                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
                    assertEquals(timestamp + 500, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 500, recordSet.getTimestampInMillis(1));
                    assertEquals(130, recordSet.getDecimalMantissa(2));
                    assertEquals(-1, recordSet.getDecimalExponent(2));
                    assertEquals(9, recordSet.getLong(3));
                    
                    assertFalse(recordSet.next());
                }
            }

        } finally {

            server.shutdown();
        }
    }
    
    @Test
    public void testReadWithEmptyInPredicate() throws Exception {

        Configuration configuration = Configuration.newBuilder()
                                                   .commitLogDirectory(this.testDirectory.resolve("commitLog"))
                                                   .dataDirectory(this.testDirectory.resolve("data"))
                                                   .build();

        HorizonServer server = new HorizonServer(configuration);

        try {

            server.start();

            try (HorizonDB client = HorizonDB.newBuilder(configuration.getPort()).setQueryTimeoutInSeconds(120).build()) {

                Connection connection = client.newConnection();
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE volume IN ();")) {

                    assertFalse(recordSet.next());
                }
            }

        } finally {

            server.shutdown();
        }
    }
    
    @Test
    public void testReadWithNotEqualsPredicate() throws Exception {

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
                
                createAndFillTimeSeries(connection);

                try (RecordSet recordSet = connection.execute("SELECT * FROM DAX WHERE volume != 6;")) {

                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
                    assertEquals(timestamp + 360, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 360, recordSet.getTimestampInMillis(1));
                    assertEquals(125, recordSet.getDecimalMantissa(2));
                    assertEquals(-1, recordSet.getDecimalExponent(2));
                    assertEquals(4, recordSet.getLong(3));

                    assertTrue(recordSet.next());
                    assertEquals(1, recordSet.getType());
                    assertEquals(timestamp + 500, recordSet.getTimestampInMillis(0));
                    assertEquals(timestamp + 500, recordSet.getTimestampInMillis(1));
                    assertEquals(130, recordSet.getDecimalMantissa(2));
                    assertEquals(-1, recordSet.getDecimalExponent(2));
                    assertEquals(9, recordSet.getLong(3));
                    
                    assertFalse(recordSet.next());
                }
            }

        } finally {

            server.shutdown();
        }
    }
    
    private static void createAndFillTimeSeries(Connection connection) {
        
        
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
    }

    private static void assertError(int errorCode, String msgFragment, HorizonDBException e) {
        assertEquals(errorCode, e.getCode());
        assertTrue(e.getMessage().contains(msgFragment));
    }    
    
    private void assertTimeSeriesFileExist(String databaseName,
                                           String timeSeriesName,
                                           long timeSeriesStart) {
        
        List<File> databasesDirectories = listFiles(databaseName, this.testDirectory.resolve("data").toFile());
        assertFalse("No database directory exists for the database: " + databaseName, databasesDirectories.isEmpty());

        List<File> timeSeriesDirectories = listFiles(timeSeriesName, databasesDirectories.toArray(new File[0]));
        assertFalse("No time series directory exists for the time series: " + timeSeriesName, 
                    timeSeriesDirectories.isEmpty());
        
        String partitionName = timeSeriesName + '-' + timeSeriesStart;
        List<File> timeSeriesFiles = listFiles(partitionName, 
                                               timeSeriesDirectories.toArray(new File[0]));
        
        assertFalse("No time series partition file exists for the partition: " + partitionName, 
                    timeSeriesFiles.isEmpty());
    }

    private static List<File> listFiles(String prefix, File... directories) {
        
        List<File> fileList = new ArrayList<>();
        
        for (File directory : directories) {
            
            File[] files = directory.listFiles(new FilenamePrefixFilter(prefix));
            
            for (File file : files) {
                fileList.add(file);
            }
        }
        
        return fileList;
    }
    
    /**
     * A <code>FilenameFilter</code> that filter files based on the start of their name.
     *
     */
    private static class FilenamePrefixFilter implements FilenameFilter {

        /**
         * The file name prefix to use when filtering.
         */
        private final String prefix;

        /**
         * Creates a new <code>FilenamePrefixFilter</code> that filter out files that do not
         * have a name starting with the specified prefix.
         * @param prefix the filename prefix
         */
        public FilenamePrefixFilter(String prefix) {
            notEmpty(prefix, "the prefix parameter must not be empty.");
            this.prefix = prefix;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean accept(File dir, String name) {
            return name.startsWith(this.prefix);
        }
        
    }
}
