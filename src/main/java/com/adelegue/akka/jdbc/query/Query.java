package com.adelegue.akka.jdbc.query;

import com.adelegue.akka.jdbc.connection.SqlConnection;

import java.sql.PreparedStatement;

/**
 * Created by adelegue on 30/04/2016.
 */
public interface Query {

    PreparedStatement buildPreparedStatement(SqlConnection connection);

}
