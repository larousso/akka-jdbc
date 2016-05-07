package com.adelegue.akka.jdbc.query;

import akka.stream.ActorAttributes;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import com.adelegue.akka.jdbc.connection.SqlConnection;
import com.adelegue.akka.jdbc.exceptions.ExceptionsHandler;
import com.adelegue.akka.jdbc.function.ResultSetExtractor;
import scala.concurrent.Future;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

/**
 * Created by adelegue on 01/05/2016.
 */
public abstract class AbstractQueryBuilder<T, Out> implements Query {

    final Future<SqlContext> sqlContext;

    protected final String sql;

    final Integer resultSetType;

    final Integer resultSetConcurrency;

    final Integer resultSetHoldability;

    final List<Object> params;

    final Source<?, ?> depends;

    final ResultSetExtractor<Out> resultSetExtractor;

    final Optional<Transaction> transaction;


    public AbstractQueryBuilder(String sql, Future<SqlContext> sqlContext, Integer resultSetType, Integer resultSetConcurrency, Integer resultSetHoldability, List<Object> params, Source<?, ?> depends, ResultSetExtractor<Out> resultSetExtractor, Optional<Transaction> transaction) {
        this.sqlContext = sqlContext;
        this.sql = sql;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
        this.params = params;
        this.depends = depends;
        this.resultSetExtractor = resultSetExtractor;
        this.transaction = transaction;
    }

    abstract T constructor(String sql, Future<SqlContext> sqlContext, Integer resultSetType, Integer resultSetConcurrency, Integer resultSetHoldability, List<Object> params, Source<?, ?> depends, ResultSetExtractor<Out> resultSetExtractor, Optional<Transaction> transaction) ;


    @Override
    public PreparedStatement buildPreparedStatement(SqlConnection sqlConnection) {
        return ExceptionsHandler.handleChecked(() -> {
            PreparedStatement preparedStatement;
            Connection connection = sqlConnection.connection();
            if(resultSetConcurrency != null && resultSetType != null && resultSetHoldability != null) {
                preparedStatement = connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
            } else if(resultSetConcurrency != null && resultSetType != null) {
                preparedStatement = connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
            } else if(resultSetType != null) {
                preparedStatement = connection.prepareStatement(sql, resultSetType);
            } else {
                preparedStatement = connection.prepareStatement(sql);
            }
            if (params != null && !params.isEmpty()) {
                for (int i = 0; i < params.size(); i++) {
                    preparedStatement.setObject(i+1, params.get(i));
                }
            }
            return preparedStatement;
        });
    }


    public T withResultSetType(Integer resultSetType) {
        return constructor(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, resultSetExtractor, transaction);
    }

    public T withResultSetConcurrency(Integer resultSetConcurrency) {
        return constructor(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, resultSetExtractor, transaction);
    }

    public T dependsOn(Source<?, ?> depends) {
        return constructor(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, resultSetExtractor, transaction);
    }

    public T runAfter(Source<?, ?> depends) {
        return constructor(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, resultSetExtractor, transaction);
    }

    public T params(List<Object> params) {
        return constructor(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, resultSetExtractor, transaction);
    }

    public T params(Object... params) {
        return constructor(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, Arrays.asList(params), depends, resultSetExtractor, transaction);
    }

    public T param(Object param) {
        if(params == null) {
            return constructor(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, unmodifiableList(singletonList(param)), depends, resultSetExtractor, transaction);
        } else {
            return constructor(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, unmodifiableList(Stream.concat(params.stream(), Stream.of(param)).collect(Collectors.toList())), depends, resultSetExtractor, transaction);
        }
    }

    public T doCommit() {
        return constructor(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, resultSetExtractor, Optional.of(Transaction.COMMIT));
    }

    public T andRollback() {
        return constructor(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, resultSetExtractor, Optional.of(Transaction.ROLLBACK));
    }

    protected  <Mat> Source<Out, Mat> applyDispatcher(Source<Out, Mat> aSource, Optional<String> dispatcher) {
        return dispatcher.map(d -> aSource.withAttributes(ActorAttributes.dispatcher(d))).orElse(aSource);
    }
    public <In, Mat> Flow<In, Out, Mat> applyDispatcher(Flow<In, Out, Mat> aFlow, Optional<String> dispatcher) {
        return dispatcher.map(d -> aFlow.withAttributes(ActorAttributes.dispatcher(d))).orElse(aFlow);
    }

    /*
    GETTERS
     */

    public Future<SqlContext> getSqlContext() {
        return sqlContext;
    }

    public String getSql() {
        return sql;
    }

    public Integer getResultSetType() {
        return resultSetType;
    }

    public Integer getResultSetConcurrency() {
        return resultSetConcurrency;
    }

    public Integer getResultSetHoldability() {
        return resultSetHoldability;
    }

    public List<Object> getParams() {
        return params;
    }

    public Source<?, ?> getDepends() {
        return depends;
    }
}
