package com.bedatadriven.renjin.table.buffer;

import com.bedatadriven.renjin.table.ColumnMetadata;
import com.bedatadriven.renjin.table.RowCursor;

import java.sql.SQLException;

public final class ColumnBuffers {

  private ColumnBuffers() {
  }

  public static ColumnBuffer create(ColumnMetadata column, int size) {
    switch(column.getColumnType()) {
    case STRING:
        return new StringBuf(size);
      case DOUBLE:
        return new DoubleBuf(size);
      case LOGICAL:
      case INT:
        return new IntBuf(size);
      case DATE:
        break;
    }
    throw new UnsupportedOperationException("Column type: " + column);
  }

  public static class DoubleBuf implements ColumnBuffer {
    private double buffer[];

    public DoubleBuf(int size) {
      this.buffer = new double[size];
    }

    @Override
    public void store(RowCursor cursor, int columnIndex, int offset) throws SQLException {
      buffer[offset] = cursor.getDouble(columnIndex);
    }

    public double get(int index) {
      return buffer[index];
    }
  }

  public static class IntBuf implements ColumnBuffer {
    private int buffer[];

    public IntBuf(int size) {
      this.buffer = new int[size];
    }

    @Override
    public void store(RowCursor cursor, int columnIndex, int offset) throws SQLException {
      buffer[offset] = cursor.getInt(columnIndex);
    }

    public int get(int index) {
      return buffer[index];
    }
  }


  public static class StringBuf implements ColumnBuffer {
    private String buffer[];

    public StringBuf(int size) {
      this.buffer = new String[size];
    }

    @Override
    public void store(RowCursor cursor, int columnIndex, int offset) throws SQLException {
      buffer[offset] = cursor.getString(columnIndex);
    }

    public String get(int index) {
      return buffer[index];
    }
  }
}
