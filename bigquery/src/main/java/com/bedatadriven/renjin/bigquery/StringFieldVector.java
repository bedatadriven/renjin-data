package com.bedatadriven.renjin.bigquery;

import com.google.api.services.bigquery.model.Table;
import org.renjin.sexp.AttributeMap;
import org.renjin.sexp.StringVector;

public class StringFieldVector extends StringVector {

  private final BigQueryTable table;
  private String fieldName;

  public StringFieldVector(BigQueryTable table, String fieldName, AttributeMap attributeMap) {
    super(attributeMap);
    this.table = table;

    this.fieldName = fieldName;
  }

  public StringFieldVector(BigQueryTable table, String fieldName) {
    this(table, name, AttributeMap.EMPTY);
  }

  @Override
  public int length() {
    return table.getNumRows();
  }

  @Override
  protected StringVector cloneWithNewAttributes(AttributeMap attributes) {
    return new StringFieldVector(table, fieldName, attributes);
  }

  @Override
  public String getElementAsString(int index) {
    return null;
  }

  @Override
  public boolean isConstantAccessTime() {
    return false;
  }
}
