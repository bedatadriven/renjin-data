package com.bedatadriven.renjin.jdbc;

import org.renjin.sexp.ListVector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Jdbc {
  public static JdbcDatabase connect(String url) throws SQLException {
    Connection connection = DriverManager.getConnection(url);
    return new JdbcDatabase(connection, new SqlDialect());
  }

}
