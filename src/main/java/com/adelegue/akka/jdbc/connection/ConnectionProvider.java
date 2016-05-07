package com.adelegue.akka.jdbc.connection;

import scala.concurrent.Future;

/**
 * Created by adelegue on 30/04/2016.
 */
public interface ConnectionProvider {

    Future<SqlConnection> getConnection();

}
