package com.bedatadriven.renjin.table.columns;

import com.bedatadriven.renjin.table.TableView;
import org.renjin.sexp.AttributeMap;
import org.renjin.sexp.DoubleVector;
import org.renjin.sexp.SEXP;

public class DoubleColumn extends DoubleVector {

  private TableView tableView;
  private int columnIndex;

  public DoubleColumn(TableView tableView, int columnIndex, AttributeMap attributeMap) {
    super(attributeMap);
    this.tableView = tableView;
    this.columnIndex = columnIndex;
  }

  @Override
  protected SEXP cloneWithNewAttributes(AttributeMap attributes) {
    return new DoubleColumn(tableView, columnIndex, attributes);
  }

  @Override
  public double getElementAsDouble(int index) {
    return tableView.getDouble(index, columnIndex);
  }

  @Override
  public boolean isConstantAccessTime() {
    return false;
  }

  @Override
  public int length() {
    return (int)tableView.getRowCount();
  }
}
