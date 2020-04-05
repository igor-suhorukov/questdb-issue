package io.questdb.sql;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.griffin.CompiledQuery;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlExecutionContextImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

public class TruncatePGDialectTruncateOnlyTest {
    @Test
    void quoteSqlId(@TempDir Path tempDir) throws Exception{
        SqlExecutionContextImpl executionContext = new SqlExecutionContextImpl();
        DefaultCairoConfiguration configuration = new DefaultCairoConfiguration(tempDir.toAbsolutePath().toString());
        try (CairoEngine engine = new CairoEngine(configuration)) {
            try (SqlCompiler compiler = new SqlCompiler(engine)) {
                CompiledQuery createTable = compiler.compile("create table tr_table(id long,name string)", executionContext);
                compiler.compile("truncate table tr_table", executionContext);
                compiler.compile("truncate table only tr_table", executionContext);//SqlException: [15] table 'only' does not exist
                /*
                  "truncate table only YOUR_TABLE_NAME" is  emitted by apache spark3 for jdbc write operation into QuestDB(PG driver) from DataSet/DataFrame in overwrite mode

                  https://www.postgresql.org/docs/current/sql-truncate.html
                  If ONLY is specified before the table name, only that table is truncated. If ONLY is not specified, the table and all its descendant tables (if any) are truncated.
                 */
            }
        }
    }
}
