package com.bedatadriven.renjin.table;

import java.util.List;

/**
 * Interface to data stored in a flat row, column format
 */
public interface TableView {

  long getRowCount();

  List<ColumnMetadata> getColumnMetadata();

  int getInt(int rowIndex, int colIndex);

  String getString(int rowIndex, int colIndex);

  double getDouble(int rowIndex, int colIndex);
}
