package com.bedatadriven.renjin.jdbc;

import com.bedatadriven.renjin.table.ColumnType;
import com.bedatadriven.renjin.table.ColumnMetadata;
import com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class JdbcRowSourceProvider {

  private final JdbcDatabase database;

  public JdbcRowSourceProvider(JdbcDatabase database) {
    this.database = database;
  }

  public JdbcDatabase getDatabase() {
    return database;
  }

  public JdbcRowSource getRowSource(String tableName) {
    List<ColumnMetadata> columns = queryColumns(tableName);
    return new JdbcRowSource(database, tableName, columns);
  }

  public List<ColumnMetadata> queryColumns(String tableName) {

    ResultSet rs = null;
    List<ColumnMetadata> columns = Lists.newArrayList();

    try {
      rs = database.getConnection().getMetaData().getColumns(null, null, tableName, null);
    } catch (SQLException e) {
      throw new RuntimeException("Could not retrieve the columns");
    }
    try {
      while(rs.next()) {
        String columnName = rs.getString("COLUMN_NAME");
        int dataType = rs.getInt("DATA_TYPE");
        int ordinal = rs.getInt("ORDINAL_POSITION");

        columns.add(new ColumnMetadata(columnName, ordinal-1, vectorType(dataType)));
      }
      rs.close();
    } catch (SQLException e) {
      throw new RuntimeException("Could not retrieve columns", e);
    }
    return columns;
  }

  private static ColumnType vectorType(int dataType) {
    switch(dataType) {
      case Types.TINYINT:
      case Types.INTEGER:
        return ColumnType.INT;

      case Types.BIT:
      case Types.BOOLEAN:
        return ColumnType.LOGICAL;

      case Types.NVARCHAR:
      case Types.CHAR:
      case Types.NCHAR:
      case Types.VARCHAR:
        return ColumnType.STRING;

      case Types.DOUBLE:
      case Types.BIGINT:
      case Types.DECIMAL:
        return ColumnType.DOUBLE;
    }
    throw new UnsupportedOperationException("Type: " + dataType);
  }
}
