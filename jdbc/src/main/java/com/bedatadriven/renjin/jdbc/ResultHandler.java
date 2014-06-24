package com.bedatadriven.renjin.jdbc;


import java.sql.ResultSet;
import java.sql.SQLException;

public interface ResultHandler<T> {

  T apply(ResultSet rs) throws SQLException;

}
