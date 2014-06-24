package com.bedatadriven.renjin.table;

import com.bedatadriven.renjin.table.columns.*;
import org.renjin.sexp.*;

/**
 * Creates an R data.frame object from a {@code TableSource}
 * or a buffered {@code TableView}
 */
public class DataFrameBuilder {

  /**
   * Creates a view of a buffered TableView
   */
  public static ListVector createView(TableView tableView) {

    ListVector.NamedBuilder frame = new ListVector.NamedBuilder();

    // create the ColumnViews
    for(ColumnMetadata column : tableView.getColumnMetadata()) {
      frame.add(column.getName(), createColumnView(tableView, column));
    }
    frame.setAttribute(Symbols.CLASS, StringVector.valueOf("data.frame"));
    frame.setAttribute(Symbols.ROW_NAMES, new RowNamesVector(tableView, AttributeMap.EMPTY));
    return frame.build();
  }

  private static Vector createColumnView(TableView tableView, ColumnMetadata column) {
    switch (column.getColumnType()) {
      case STRING:
        return new StringColumn(tableView, column.getIndex());
      case DOUBLE:
        return new DoubleColumn(tableView, column.getIndex(), AttributeMap.EMPTY);
      case INT:
        return new IntColumn(tableView, column.getIndex(), AttributeMap.EMPTY);
      case DATE:
        return new IntColumn(tableView, column.getIndex(), dateAttributes());
      case LOGICAL:
        return new NullableBooleanColumn(tableView, column.getIndex(), AttributeMap.EMPTY);
    }
    throw new IllegalArgumentException("unsupported type: " + column.getColumnType().name());
  }

  private static AttributeMap dateAttributes() {
    return AttributeMap.builder().setClass("Date").build();
  }
}
