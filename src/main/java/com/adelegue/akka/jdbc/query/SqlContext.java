package com.adelegue.akka.jdbc.query;

import akka.actor.ActorSystem;
import com.adelegue.akka.jdbc.connection.SqlConnection;

import java.util.Optional;

/**
 * Created by adelegue on 30/04/2016.
 */
public class SqlContext {

    public final ActorSystem actorSystem;

    public final SqlConnection connection;

    public final Optional<String> dispatcher;

    public SqlContext(ActorSystem actorSystem, SqlConnection connection, Optional<String> dispatcher) {
        this.actorSystem = actorSystem;
        this.connection = connection;
        this.dispatcher = dispatcher;
    }
}
