package com.bedatadriven.renjin.bigquery;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableFieldSchema;
import org.renjin.sexp.ListVector;

import java.io.IOException;
import java.math.BigInteger;

public class BigQueryTable {
  private Bigquery client;
  private final Table table;

  public BigQueryTable(Bigquery client, String projectId, String datasetId, String tableName) throws IOException {
    this.client = client;
    table = client.tables().get(projectId, datasetId, tableName).execute();
  }

  public int getNumRows() {
    return table.getNumRows().intValue();
  }

  public ListVector asDataFrame() {
    ListVector.NamedBuilder df = ListVector.newNamedBuilder();
    for(TableFieldSchema field : table.getSchema().getFields()) {
      //STRING, INTEGER, FLOAT, BOOLEAN, TIMESTAMP or RECORD

      if(field.getType().equals("STRING")) {
        df.add(field.getName(), new StringFieldVector(this, field.getName()));

      } else if(field.getType().equals("INTEGER")) {
        df.add(field.getName(), new StringFieldVector(this, field.getName()));
      }

    }
    throw new UnsupportedOperationException();
  }

}
