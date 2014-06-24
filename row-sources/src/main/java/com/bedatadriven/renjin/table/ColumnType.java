package com.bedatadriven.renjin.table;

/**
 * Supported types of Columns
 */
public enum ColumnType {

  STRING,

  DOUBLE,

  INT,

  /**
   * ISO Date
   *
   * RowCursor implementations should supply date values as the number
   * of days since 1970-01-01, with negative values for earlier dates.
   *
   * This is the definition used by the R "Date" class.
   *
   */
  DATE,


  LOGICAL;
}
