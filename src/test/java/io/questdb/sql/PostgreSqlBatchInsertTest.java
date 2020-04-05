package io.questdb.sql;

import io.questdb.ServerMain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class PostgreSqlBatchInsertTest {
    @Test
    void batchInsert(@TempDir Path tempDir) throws Exception{
        ServerMain.main(new String[]{"-d", tempDir.toString()});
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
}
