package com.adelegue.akka.jdbc.query;

import akka.NotUsed;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import com.adelegue.akka.jdbc.stream.source.SelectQuerySource;
import com.adelegue.akka.jdbc.utils.ResultSetExtractor;
import scala.concurrent.Future;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
 *
 * Created by adelegue on 29/04/2016.
 */
public class SelectQueryBuilder<Out> extends AbstractQueryBuilder<SelectQueryBuilder<Out>, Out> {

    public SelectQueryBuilder(String sql, Future<SqlContext> sqlContext, ResultSetExtractor<Out> resultSetExtractor) {
        super(sql, sqlContext, null, null, null, null, null, resultSetExtractor, Optional.empty());
    }

    SelectQueryBuilder(String sql, Future<SqlContext> sqlContext, Integer resultSetType, Integer resultSetConcurrency, Integer resultSetHoldability, List<Object> params, Source<?, ?> depends, ResultSetExtractor<Out> resultSetExtractor, Optional<Transaction> transaction) {
        super(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, resultSetExtractor, transaction);
    }

    @Override
    SelectQueryBuilder<Out> constructor(String sql, Future<SqlContext> sqlContext, Integer resultSetType, Integer resultSetConcurrency, Integer resultSetHoldability, List<Object> params, Source<?, ?> depends, ResultSetExtractor<Out> resultSetExtractor, Optional<Transaction> transaction) {
        return new SelectQueryBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, resultSetExtractor, transaction);
    }

    public <Out2> SelectQueryBuilder<Out2> as(ResultSetExtractor<Out2> resultSetExtractor) {
        return new SelectQueryBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, resultSetExtractor, transaction);
    }

    public <Out2> Source<Out2, ?> get(ResultSetExtractor<Out2> rsExtract) {
        return as(rsExtract).get();
    }

    public Source<Out, ?> get() {
        Source<Out, NotUsed> querySource = Source.fromFuture(sqlContext).flatMapMerge(1, ctx ->
                applyDispatcher(Source.fromGraph(new SelectQuerySource<>(ctx, this, resultSetExtractor, transaction)), ctx.dispatcher)
        );
        if(depends != null) {
            return depends.fold(0, (acc, elt) -> acc + 1).flatMapMerge(1, i -> querySource);
        } else {
            return querySource;
        }
    }

    public <In> Flow<In, Out, ?> grabInParams(Function<In, List<?>> convertParams) {
        return Flow.<In>create().flatMapMerge(1, in -> this.params(convertParams.apply(in)).get());
    }

    public <In> Flow<In, Out, ?> grabInParam(Function<In, ?> convertParams) {
        return Flow.<In>create().flatMapMerge(1, in -> this.param(convertParams.apply(in)).get());
    }
}
