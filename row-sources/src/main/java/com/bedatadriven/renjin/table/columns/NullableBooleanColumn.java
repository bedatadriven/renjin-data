package com.bedatadriven.renjin.table.columns;

import com.bedatadriven.renjin.table.TableView;
import org.renjin.sexp.AttributeMap;
import org.renjin.sexp.LogicalVector;
import org.renjin.sexp.SEXP;

public class NullableBooleanColumn extends LogicalVector {

  private final TableView view;
  private int columnIndex;

  public NullableBooleanColumn(TableView view, int columnIndex, AttributeMap attributeMap) {
    super(attributeMap);
    this.view = view;
    this.columnIndex = columnIndex;
  }

  @Override
  public int length() {
    return (int)view.getRowCount();
  }

  @Override
  public int getElementAsRawLogical(int index) {
    return view.getInt(index, columnIndex);
  }

  @Override
  public boolean isConstantAccessTime() {
    return true;
  }

  @Override
  protected SEXP cloneWithNewAttributes(AttributeMap attributes) {
    return new NullableBooleanColumn(view, columnIndex, attributes);
  }
}
