package com.bedatadriven.renjin.table.columns;

import com.bedatadriven.renjin.table.TableView;
import org.renjin.sexp.AttributeMap;
import org.renjin.sexp.IntVector;
import org.renjin.sexp.SEXP;

public class RowNamesVector extends IntVector {

  private TableView tableView;

  public RowNamesVector(TableView tableView, AttributeMap attributes) {
    super(attributes);
    this.tableView = tableView;
  }

  @Override
  public int length() {
    return (int)tableView.getRowCount();
  }

  @Override
  public int getElementAsInt(int i) {
    return i+1;
  }

  @Override
  public boolean isConstantAccessTime() {
    return true;
  }

  @Override
  protected SEXP cloneWithNewAttributes(AttributeMap attributes) {
    return new RowNamesVector(tableView, attributes);
  }
}
