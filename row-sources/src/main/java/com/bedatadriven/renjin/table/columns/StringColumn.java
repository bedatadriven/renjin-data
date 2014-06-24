package com.bedatadriven.renjin.table.columns;

import com.bedatadriven.renjin.table.TableView;
import org.renjin.sexp.AttributeMap;
import org.renjin.sexp.StringVector;

public class StringColumn extends StringVector {

  private final TableView tableView;
  private final int columnIndex;

  public StringColumn(TableView tableView, int columnIndex, AttributeMap attributes) {
    super(attributes);
    this.tableView = tableView;
    this.columnIndex = columnIndex;
  }

  public StringColumn(TableView tableView, int columnIndex) {
    this(tableView, columnIndex, AttributeMap.EMPTY);
  }

  @Override
  public int length() {
    return (int)tableView.getRowCount();
  }

  @Override
  protected StringVector cloneWithNewAttributes(AttributeMap attributes) {
    return new StringColumn(tableView, columnIndex, attributes);
  }

  @Override
  public String getElementAsString(int index) {
    return tableView.getString(index, columnIndex);
  }

  @Override
  public boolean isConstantAccessTime() {
    return true;
  }
}
