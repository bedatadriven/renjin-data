package com.bedatadriven.renjin.jdbc;

import com.bedatadriven.renjin.table.DataFrameBuilder;
import com.bedatadriven.renjin.table.buffer.PagingTableBuffer;
import org.junit.Test;
import org.renjin.script.RenjinScriptEngine;
import org.renjin.script.RenjinScriptEngineFactory;
import org.renjin.sexp.ListVector;

import javax.script.ScriptException;
import java.sql.SQLException;

public class JdbcTest {

  @Test
  public void test() throws SQLException, ScriptException {
    JdbcDatabase db = Jdbc.connect("jdbc:mysql://127.0.0.1:3306/dhiraagu?user=root&password=root");
    JdbcRowSourceProvider provider = new JdbcRowSourceProvider(db);

    JdbcRowSource voice = provider.getRowSource("voice");
    PagingTableBuffer tableBuffer = new PagingTableBuffer(voice);

    ListVector df = DataFrameBuilder.createView(tableBuffer);

    RenjinScriptEngine engine = new RenjinScriptEngineFactory().getScriptEngine();
    engine.put("df", df);
    engine.eval("print(head(df))");

    engine.eval("df$roaming <- as.logical(df$roaming)");

  }
}
