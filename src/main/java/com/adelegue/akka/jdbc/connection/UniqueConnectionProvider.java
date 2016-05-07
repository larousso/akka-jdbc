package com.adelegue.akka.jdbc.connection;

import akka.dispatch.Futures;
import scala.concurrent.Future;

/**
 * Created by adelegue on 30/04/2016.
 */
public class UniqueConnectionProvider implements ConnectionProvider {

    final SqlConnection connection;

    public UniqueConnectionProvider(SqlConnection connection) {
        this.connection = connection;
    }

    @Override
    public Future<SqlConnection> getConnection() {
        return Futures.successful(connection);
    }
}

