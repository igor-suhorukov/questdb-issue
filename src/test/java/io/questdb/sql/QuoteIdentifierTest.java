package io.questdb.sql;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

public class QuoteIdentifierTest {
    @Test
    void quoteSqlId(@TempDir Path tempDir) throws Exception{
        SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl();
        DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(tempDir.toAbsolutePath().toString());
        try (CairoEngine engine = new CairoEngine(configuration)) {
            try (SqlCompiler compiler = new SqlCompiler(engine)) {
                CompiledQuery createTable = compiler.compile("create table \"quoted_table\"(\"id\" long,\"name\" string)", executionContext);
                compiler.compile("select * from quoted_table", executionContext);
                compiler.compile("insert into quoted_table(\"id\",\"name\") values (1,'String')",executionContext);
                compiler.compile("insert into quoted_table(id,name) values (2,'Other string')",executionContext); // InvalidColumnException
                compiler.compile("insert into \"quoted_table\"(\"id\",\"name\") values (3,'Base string')",executionContext); // SqlException: [12] literal expected
                compiler.compile("select \"id\" from quoted_table", executionContext);
                compiler.compile("select \"name\" from quoted_table", executionContext);
                compiler.compile("select id from quoted_table", executionContext); //SqlException: [7] Invalid column: id
                compiler.compile("select name from quoted_table", executionContext); //SqlException: [7] Invalid column: name
            }
        }
    }
}
