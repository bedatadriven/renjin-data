package com.bedatadriven.renjin.table;

import com.google.common.base.Optional;

import java.util.List;

/**
 * General interface to Table-like Data sources
 * stored in row-major order.
 *
 */
public interface RowSource {

  /**
   *
   * @return the list of columns present in this {@code RowSource}
   */
  List<ColumnMetadata> getColumnMetadata();


  /**
   * Opens a cursor, starting at the given offset, and limited
   * to
   * @param offset zero-based offset from the beginning of the row set
   * @param limit
   * @return
   */
  RowCursor openCursor(int offset, int limit) throws Exception;


  /**
   * @return the count of rows in this RowSource if already available
   */
  Optional<Long> getRowCount();

  /**
   * @return counts the rows within this {@code RowSource}
   */
  Long countRows() throws Exception;

}
