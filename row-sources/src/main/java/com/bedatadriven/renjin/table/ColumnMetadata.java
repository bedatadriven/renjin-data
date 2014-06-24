package com.bedatadriven.renjin.table;

/**
 * Describes a column within a {@code RowSource}
 */
public class ColumnMetadata {
  private final String name;
  private final int index;
  private final ColumnType columnType;

  public ColumnMetadata(String name, int index, ColumnType type) {
    this.name = name;
    this.index = index;
    this.columnType = type;
  }

  public String getName() {
    return name;
  }

  public int getIndex() {
    return index;
  }

  public ColumnType getColumnType() {
    return columnType;
  }
}
