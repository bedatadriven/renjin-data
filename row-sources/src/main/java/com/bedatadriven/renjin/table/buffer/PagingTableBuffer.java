package com.bedatadriven.renjin.table.buffer;

import com.bedatadriven.renjin.table.ColumnMetadata;
import com.bedatadriven.renjin.table.RowCursor;
import com.bedatadriven.renjin.table.RowSource;
import com.bedatadriven.renjin.table.TableView;

import java.util.List;

/**
 * A buffer for {@code RowSource}s which loads one page at a time from
 * the database into memory
 */
public class PagingTableBuffer implements TableView {

  public static final int DEFAULT_PAGE_SIZE = 1000;
  private final RowSource table;

  private int pageOffset = Integer.MIN_VALUE;
  private int pageSize = DEFAULT_PAGE_SIZE;

  private ColumnBuffer[] buffers;

  public PagingTableBuffer(RowSource table) {
    this.table = table;

    // Create column buffers
    List<ColumnMetadata> columns = table.getColumnMetadata();

    buffers = new ColumnBuffer[columns.size()];
    for(int i=0;i!=columns.size();++i) {
      buffers[i] = ColumnBuffers.create(columns.get(i), pageSize);
    }
  }

  @Override
  public List<ColumnMetadata> getColumnMetadata() {
    return table.getColumnMetadata();
  }

  /**
   * Queries and fills our buffer based on the current
   * {@code pageOffset} and {@code pageSize}
   */
  private void fillBuffer() {

    try {
      RowCursor cursor = table.openCursor(pageOffset, pageSize);

      int offset = 0;
      while(cursor.next()) {
        for(int j=0;j!=buffers.length;++j) {
          buffers[j].store(cursor, j, offset);
        }
        offset++;
      }
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long getRowCount() {
    if(table.getRowCount().isPresent()) {
      return table.getRowCount().get();
    } else {
      try {
        return table.countRows();
      } catch (Exception e) {
        throw new RuntimeException("Exception encountered while trying to count rows in data source", e);
      }
    }
  }

  @Override
  public int getInt(int row, int col) {
    ensureInBuffer(row);
    return ((ColumnBuffers.IntBuf)buffers[col]).get(row - pageOffset);
  }

  @Override
  public String getString(int row, int col) {
    ensureInBuffer(row);
    return ((ColumnBuffers.StringBuf)buffers[col]).get(row - pageOffset);
  }

  public double getDouble(int row, int col) {
    ensureInBuffer(row);
    return ((ColumnBuffers.DoubleBuf)buffers[col]).get(row - pageOffset);
  }

  /**
   *
   * @return the index of the page on which the given {@code rowIndex} would fall
   */
  int pageForRow(int rowIndex) {
    return (int)Math.ceil(rowIndex / pageSize);
  }

  private void ensureInBuffer(int rowIndex) {
    if(rowIndex < pageOffset || rowIndex >= (pageOffset+pageSize)) {
      pageOffset = (pageForRow(rowIndex) * pageSize);
      fillBuffer();
    }
  }
}
