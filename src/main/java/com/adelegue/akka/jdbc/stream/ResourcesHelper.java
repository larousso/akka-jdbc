package com.adelegue.akka.jdbc.stream;

import com.adelegue.akka.jdbc.connection.SqlConnection;
import com.adelegue.akka.jdbc.query.Transaction;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

/**
 * Mixin to handle jdbc resources.
 *
 * Created by adelegue on 01/05/2016.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public interface ResourcesHelper {

    default void commit(SqlConnection sqlConnection, Optional<Transaction> transaction, Boolean onError) throws SQLException {
        //On error and transaction or rollback required => rollback
        if( (Boolean.TRUE.equals(onError) || transaction.map(Transaction.ROLLBACK::equals).orElse(Boolean.FALSE)) && !sqlConnection.connection().getAutoCommit()) {
            sqlConnection.connection().rollback();
        }

        //No error, Transaction opened and commit => commit
        if(!Boolean.TRUE.equals(onError) && transaction.map(Transaction.COMMIT::equals).orElse(Boolean.FALSE) && !sqlConnection.connection().getAutoCommit()) {
            sqlConnection.connection().commit();
        }
    }

    default void cleanResources(Statement statement, SqlConnection sqlConnection, Optional<Transaction> transaction) throws SQLException {
        cleanResources(statement, sqlConnection, transaction, Boolean.FALSE);
    }

    default void cleanResources(Statement statement, SqlConnection sqlConnection, Optional<Transaction> transaction, Boolean onError) throws SQLException {
        try {
            if(statement != null && !statement.isClosed()) {
                statement.close();
            }
        } finally {
            try {
                if(!sqlConnection.keepOpen()) {
                    //If connection will be closed, and no transaction action is specified, commit is forced:
                    commit(sqlConnection, Optional.of(transaction.orElse(Transaction.COMMIT)), onError);
                } else {
                    commit(sqlConnection, transaction, onError);
                }
            } catch (SQLException e) {
                sqlConnection.close();
            } finally {
                if(!sqlConnection.keepOpen()) {
                    sqlConnection.close();
                }
            }
        }
    }
}
