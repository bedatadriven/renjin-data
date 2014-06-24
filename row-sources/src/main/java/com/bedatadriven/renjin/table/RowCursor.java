package com.bedatadriven.renjin.table;

import java.sql.SQLException;

/**
 * An interface to a row cursor, which
 * can traverse sequentially over a set of
 * rows within a RowSource
 */
public interface RowCursor extends AutoCloseable {

  /**
   * Advances the {@code RowCursor} to the next row
   *
   * @return true if the cursor has moved to the next row
   * or false if there are now more rows
   */
  boolean next() throws SQLException;

  int getInt(int columnIndex) throws SQLException;

  double getDouble(int columnIndex) throws SQLException;

  String getString(int columnIndex) throws SQLException;

  boolean getBoolean(int columnIndex) throws SQLException;

  boolean isNull(int columnIndex) throws SQLException;

}