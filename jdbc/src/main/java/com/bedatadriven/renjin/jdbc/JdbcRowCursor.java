package com.bedatadriven.renjin.jdbc;

import com.bedatadriven.renjin.table.RowCursor;

import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcRowCursor implements RowCursor {

  private final ResultSet rs;

  public JdbcRowCursor(ResultSet rs) {
    this.rs = rs;
  }

  @Override
  public boolean next() throws SQLException {
    return rs.next();
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return rs.getInt(columnIndex+1);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return rs.getDouble(columnIndex+1);
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return rs.getString(columnIndex+1);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return rs.getBoolean(columnIndex+1);
  }

  @Override
  public boolean isNull(int columnIndex) throws SQLException {
    return rs.getObject(columnIndex+1) == null;
  }

  @Override
  public void close() throws SQLException{
    rs.close();
  }
}
