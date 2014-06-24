package com.bedatadriven.renjin.jdbc;

import com.bedatadriven.renjin.table.ColumnMetadata;
import com.bedatadriven.renjin.table.RowCursor;
import com.bedatadriven.renjin.table.RowSource;
import com.google.common.base.Optional;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;


public class JdbcRowSource implements RowSource {

  private JdbcDatabase database;
  private String tableName;
  private final List<ColumnMetadata> columns;
  private Optional<Long> rowCount = Optional.absent();

  public JdbcRowSource(JdbcDatabase database, String tableName, List<ColumnMetadata> columns) {
    this.database = database;
    this.tableName = tableName;
    this.columns = columns;
  }

  @Override
  public List<ColumnMetadata> getColumnMetadata() {
    return columns;
  }

  @Override
  public RowCursor openCursor(int offset, int limit) throws SQLException {

    StringBuilder sql = new StringBuilder();
    sql.append("SELECT ");
    boolean requiresComma = false;
    for(ColumnMetadata column : columns) {
      if(requiresComma) {
        sql.append(", ");
      }
      sql.append(column.getName());
      requiresComma = true;
    }
    sql.append(" FROM ");
    sql.append(tableName);
    sql.append(" LIMIT ").append(offset).append(", ").append(limit);

    Statement statement = database.getConnection().createStatement();
    ResultSet resultSet = statement.executeQuery(sql.toString());
    return new JdbcRowCursor(resultSet);
  }

  @Override
  public Optional<Long> getRowCount() {
    return rowCount;
  }

  @Override
  public Long countRows() throws SQLException {
    return database.executeQuery("SELECT COUNT(*) FROM " + tableName, new ResultHandler<Long>() {
      @Override
      public Long apply(ResultSet rs) throws SQLException {
        rs.next();
        return rs.getLong(1);
      }
    });
  }
}
