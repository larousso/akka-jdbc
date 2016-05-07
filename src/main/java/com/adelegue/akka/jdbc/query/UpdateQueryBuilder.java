package com.adelegue.akka.jdbc.query;

import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import com.adelegue.akka.jdbc.stream.source.UpdateQuerySource;
import com.adelegue.akka.jdbc.stream.stage.UpdateQueryStage;
import com.adelegue.akka.jdbc.utils.ResultSetExtractor;
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
                Source.fromGraph(new UpdateQuerySource(ctx, this, transaction))
        );
        if(depends != null) {
            return depends.fold(0, (acc, elt) -> acc + 1).flatMapMerge(1, i -> querySource);
        } else {
            return querySource;
        }
    }

    public <In> Flow<In, Integer, ?> toFlow() {
        return Flow.<In>create().flatMapMerge(1, in -> Source.fromFuture(sqlContext)).via(Flow.fromGraph(new UpdateQueryStage(this)));
    }


    public <In, Out> Flow<In, Out, ?> takeInParams() {
        return null;
    }

    public <In, Out, Params> Flow<In, Out, ?> takeInParams(Function<In, Params> convertParams) {
        return null;
    }

}
