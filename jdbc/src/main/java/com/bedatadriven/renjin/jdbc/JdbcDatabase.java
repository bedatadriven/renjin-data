package com.bedatadriven.renjin.jdbc;

import java.sql.*;

public class JdbcDatabase {
  private Connection connection;
  private SqlDialect dialect;

  public JdbcDatabase(Connection connection, SqlDialect dialect) {
    this.connection = connection;
    this.dialect = dialect;
  }

  public SqlDialect getDialect() {
    return dialect;
  }


  public Connection getConnection() {
    return connection;
  }

  public <T> T executeQuery(String query, ResultHandler<T> resultHandler) throws SQLException {

    // Create the statement
    Statement statement;
    try {
      statement = connection.createStatement();
    } catch (SQLException e) {
      throw new RuntimeException("Could not create a statement", e);
    }

    // Execute the query
    ResultSet resultSet;
    try {
      resultSet = statement.executeQuery(query);
    } catch (SQLException e) {
      throw new RuntimeException("Query execution failed: " + e.getMessage(), e);
    }

    // Handle the results
    try {
      return resultHandler.apply(resultSet);
    } finally {
      try {
        resultSet.close();
      } catch (SQLException e) {
        // swallow
      }
    }
  }
}
