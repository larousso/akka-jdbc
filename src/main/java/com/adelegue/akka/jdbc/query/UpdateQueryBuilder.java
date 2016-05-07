package com.adelegue.akka.jdbc.query;

import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import com.adelegue.akka.jdbc.stream.source.UpdateQuerySource;
import com.adelegue.akka.jdbc.function.ResultSetExtractor;
import scala.concurrent.Future;

import java.sql.ResultSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 * Created by adelegue on 01/05/2016.
 */
public class UpdateQueryBuilder extends AbstractQueryBuilder<UpdateQueryBuilder, Integer> {

    public UpdateQueryBuilder(String sql, Future<SqlContext> sqlContext) {
        super(sql, sqlContext, null, null, null, null, null, null, Optional.empty());
    }

    private UpdateQueryBuilder(String sql, Future<SqlContext> sqlContext, Integer resultSetType, Integer resultSetConcurrency, Integer resultSetHoldability, List<Object> params, Source<?, ?> depends, Optional<Transaction> transaction) {
        super(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, null, transaction);
    }

    @Override
    UpdateQueryBuilder constructor(String sql, Future<SqlContext> sqlContext, Integer resultSetType, Integer resultSetConcurrency, Integer resultSetHoldability, List<Object> params, Source<?, ?> depends, ResultSetExtractor<Integer> resultSetExtractor, Optional<Transaction> transaction) {
        return new UpdateQueryBuilder(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, transaction);
    }

    public UpdateQueryGeneratedKeyBuilder<ResultSet> returnGeneratedKeys() {
        return new UpdateQueryGeneratedKeyBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, ResultSetExtractor.identity(), transaction);
    }

    public Source<Integer, ?> count() {

        Source<Integer, ?> querySource = Source.fromFuture(sqlContext).flatMapMerge(1, ctx ->
                applyDispatcher(Source.fromGraph(new UpdateQuerySource(ctx, this, transaction)), ctx.dispatcher)
        );
        if(depends != null) {
            return depends.fold(0, (acc, elt) -> acc + 1).flatMapMerge(1, i -> querySource);
        } else {
            return querySource;
        }
    }

    public <In> Flow<In, Integer, ?> grabInParams(Function<In, List<?>> convertParams) {
        return Flow.<In>create().flatMapMerge(1, in -> this.params(convertParams.apply(in)).count());
    }

    public <In> Flow<In, Integer, ?> grabInParam(Function<In, ?> convertParams) {
        return Flow.<In>create().flatMapMerge(1, in -> this.param(convertParams.apply(in)).count());
    }
}
