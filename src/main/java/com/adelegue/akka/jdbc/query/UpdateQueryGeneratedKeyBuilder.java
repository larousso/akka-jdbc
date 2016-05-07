package com.adelegue.akka.jdbc.query;

import akka.NotUsed;
import akka.stream.javadsl.Source;
import com.adelegue.akka.jdbc.stream.source.UpdateQueryGeneratedKeysSource;
import com.adelegue.akka.jdbc.utils.ResultSetExtractor;
import scala.concurrent.Future;

import java.util.List;
import java.util.Optional;

/**
 * Created by adelegue on 01/05/2016.
 */
public class UpdateQueryGeneratedKeyBuilder<Out> extends AbstractQueryBuilder<UpdateQueryGeneratedKeyBuilder<Out>, Out> {

    UpdateQueryGeneratedKeyBuilder(String sql, Future<SqlContext> sqlContext, Integer resultSetType, Integer resultSetConcurrency, Integer resultSetHoldability, List<Object> params, Source<?, ?> depends, ResultSetExtractor<Out> resultSetExtractor, Optional<Transaction> transaction) {
        super(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, resultSetExtractor, transaction);
    }

    @Override
    UpdateQueryGeneratedKeyBuilder<Out> constructor(String sql, Future<SqlContext> sqlContext, Integer resultSetType, Integer resultSetConcurrency, Integer resultSetHoldability, List<Object> params, Source<?, ?> depends, ResultSetExtractor<Out> resultSetExtractor, Optional<Transaction> transaction) {
        return new UpdateQueryGeneratedKeyBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, resultSetExtractor, transaction);
    }

    public <Out2> Source<Out2, ?> get(ResultSetExtractor<Out2> rsExtract) {
        return as(rsExtract).get();
    }

    public <Out2> UpdateQueryGeneratedKeyBuilder<Out2> as(ResultSetExtractor<Out2> rsExtract) {
        return new UpdateQueryGeneratedKeyBuilder<>(sql, sqlContext, resultSetType, resultSetConcurrency, resultSetHoldability, params, depends, rsExtract, transaction);
    }

    public Source<Out, ?> get() {
        Source<Out, NotUsed> querySource = Source.fromFuture(sqlContext).flatMapMerge(1, ctx ->
                Source.fromGraph(new UpdateQueryGeneratedKeysSource<>(ctx, this, resultSetExtractor, transaction))
        );
        if(depends != null) {
            return depends.fold(0, (acc, elt) -> acc + 1).flatMapMerge(1, i -> querySource);
        } else {
            return querySource;
        }
    }

}
