package com.bedatadriven.renjin.table.buffer;

import com.bedatadriven.renjin.table.RowCursor;

public class SimpleRowCursor implements RowCursor {

  private int count;
  private int position;

  public SimpleRowCursor(int totalRowCount, int offset) {
    this.count = totalRowCount;
    this.position = offset-1;
  }

  @Override
  public boolean next() {
    if(position+1 < count) {
      position++;
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int getInt(int columnIndex) {
    return position;
  }

  @Override
  public double getDouble(int columnIndex) {
    return position;
  }

  @Override
  public String getString(int columnIndex) {
    return Integer.toHexString(columnIndex);
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    return position % 2 == 0;
  }

  @Override
  public boolean isNull(int columnIndex) {
    return false;
  }

  @Override
  public void close()  {

  }
}
