package com.bedatadriven.renjin.table.buffer;

import com.bedatadriven.renjin.table.ColumnMetadata;
import com.bedatadriven.renjin.table.ColumnType;
import com.bedatadriven.renjin.table.RowCursor;
import com.bedatadriven.renjin.table.RowSource;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

import java.util.List;

public class SimpleRowSource implements RowSource {

  private List<ColumnMetadata> columns;

  public SimpleRowSource() {
    this.columns = Lists.newArrayList();
    this.columns.add(new ColumnMetadata("A", 1, ColumnType.DOUBLE));
  }

  @Override
  public List<ColumnMetadata> getColumnMetadata() {
    return columns;
  }

  @Override
  public RowCursor openCursor(int offset, int limit) throws Exception {
    return new SimpleRowCursor(offset+limit, offset);
  }

  @Override
  public Optional<Long> getRowCount() {
    return null;
  }

  @Override
  public Long countRows() throws Exception {
    return null;
  }
}
