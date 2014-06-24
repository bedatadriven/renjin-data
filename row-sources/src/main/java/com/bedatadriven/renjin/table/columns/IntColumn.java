package com.bedatadriven.renjin.table.columns;

import com.bedatadriven.renjin.table.TableView;
import org.renjin.sexp.AttributeMap;
import org.renjin.sexp.IntVector;
import org.renjin.sexp.SEXP;

public class IntColumn extends IntVector {

  private final TableView tableView;
  private final int columnIndex;

  public IntColumn(TableView tableView, int columnIndex, AttributeMap attributeMap) {
    super(attributeMap);
    this.tableView = tableView;
    this.columnIndex = columnIndex;
  }

  @Override
  public int length() {
    return (int)tableView.getRowCount();
  }

  @Override
  public int getElementAsInt(int index) {
    return tableView.getInt(index, columnIndex);
  }

  @Override
  public boolean isConstantAccessTime() {
    return false;
  }

  @Override
  protected SEXP cloneWithNewAttributes(AttributeMap attributes) {
    return new IntColumn(tableView, columnIndex, attributes);
  }
}
