package com.adelegue.akka.jdbc.connection;

import com.adelegue.akka.jdbc.exceptions.ExceptionsHandler;

import java.sql.Connection;

/**
 * Created by adelegue on 30/04/2016.
 */
public class SqlConnection {

    private final Connection connection;

    private final String name;

    private final Boolean keepOpen;


    public SqlConnection(Connection connection, String name, Boolean keepOpen) {
        this.connection = connection;
        this.name = name;
        this.keepOpen = keepOpen;
    }

    public Connection connection() {
        return this.connection;
    }

    public String name() {
        return name;
    }

    public void close() {
        ExceptionsHandler.handleChecked0(this.connection::close);
    }

    public Boolean keepOpen() {
        return keepOpen;
    }

}
