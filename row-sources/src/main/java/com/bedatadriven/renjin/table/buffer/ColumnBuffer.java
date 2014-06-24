package com.bedatadriven.renjin.table.buffer;

import com.bedatadriven.renjin.table.RowCursor;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Objects that hold the currently-buffered contents of
 * columns.
 *
 * Although we necessarily have to buffer by row, we still
 * store by column as storing by row, for example in an {@code Object[]}
 * would require boxing all of the integers/doubles/booleans etc.
 */
public interface ColumnBuffer {

  /**
   * Stores the value of column indexed {@code columnIndex}
   * at the given {@code offset} within the buffer
   */
  void store(RowCursor cursor, int columnIndex, int offset) throws Exception;


}
