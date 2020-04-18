package io.questdb.sql;

import io.questdb.ServerMain;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class PostgreSqlBatchInsertTest {
    @BeforeAll
    static void init(@TempDir Path tempDir) throws Exception{
        ServerMain.main(new String[]{"-d", tempDir.toString()});
    }

    @Test
    void batchInsertWithTransaction() throws Exception{
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:8812/", "admin", "quest")){
            try (Statement statement = connection.createStatement()){
                statement.executeUpdate("create table test(id long,val int)");
            }
            connection.setAutoCommit(false);
            try (PreparedStatement batchInsert = connection.prepareStatement("insert into test(id,val) values(?,?)")){
                batchInsert.setLong(1,0L);
                batchInsert.setInt(2, 1);
                batchInsert.addBatch();
                batchInsert.setLong(1,1L);
                batchInsert.setInt(2, 2);
                batchInsert.addBatch();
                batchInsert.setLong(1,2L);
                batchInsert.setInt(2, 3);
                batchInsert.addBatch();
                batchInsert.clearParameters();
                long[] longs = batchInsert.executeLargeBatch();//java.sql.BatchUpdateException: Batch entry 0 insert into test(id,val) values(0,1) was aborted: ERROR: table does not exist [name=BEGIN]
                connection.commit();
            }
        }
    }

    @Test
    void largeBatchInsertMethod() throws Exception{
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:8812/", "admin", "quest")){
            try (Statement statement = connection.createStatement()){
                statement.executeUpdate("create table test_large_batch(id long,val int)");
            }
            try (PreparedStatement batchInsert = connection.prepareStatement("insert into test_large_batch(id,val) values(?,?)")){
                batchInsert.setLong(1,0L);
                batchInsert.setInt(2, 1);
                batchInsert.addBatch();
                batchInsert.setLong(1,1L);
                batchInsert.setInt(2, 2);
                batchInsert.addBatch();
                batchInsert.setLong(1,2L);
                batchInsert.setInt(2, 3);
                batchInsert.addBatch();
                batchInsert.clearParameters();
                long[] longs = batchInsert.executeLargeBatch();
            }
        }
    }

    @Test
    void regularBatchInsertMethod() throws Exception{
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:8812/", "admin", "quest")){
            try (Statement statement = connection.createStatement()){
                statement.executeUpdate("create table test_batch(id long,val int)");
            }
            try (PreparedStatement batchInsert = connection.prepareStatement("insert into test_batch(id,val) values(?,?)")){
                batchInsert.setLong(1,0L);
                batchInsert.setInt(2, 1);
                batchInsert.addBatch();
                batchInsert.setLong(1,1L);
                batchInsert.setInt(2, 2);
                batchInsert.addBatch();
                batchInsert.setLong(1,2L);
                batchInsert.setInt(2, 3);
                batchInsert.addBatch();
                batchInsert.clearParameters();
                int[] longs = batchInsert.executeBatch();
            }
        }
    }

}
