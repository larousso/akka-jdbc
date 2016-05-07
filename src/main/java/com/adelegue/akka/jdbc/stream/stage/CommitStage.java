package com.adelegue.akka.jdbc.stream.stage;

import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.*;
import com.adelegue.akka.jdbc.connection.SqlConnection;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Future;

import java.util.concurrent.CompletionStage;

/**
 * Created by adelegue on 30/04/2016.
 */
public class CommitStage<T> extends GraphStage<FlowShape<T, T>> {

    private final Inlet<T> in = Inlet.create("CommitStage.in");
    private final Outlet<T> out = Outlet.create("CommitStage.out");
    private final FlowShape<T, T> shape = FlowShape.of(in, out);

    private final CompletionStage<SqlConnection> sqlConnection;

    private final Boolean onEach;

    public CommitStage(Future<SqlConnection> sqlConnection, Boolean onEach) {
        this.sqlConnection = FutureConverters.toJava(sqlConnection);
        this.onEach = onEach;
    }

    @Override
    public FlowShape<T, T> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape()) {
            Boolean first = true;
            SqlConnection currentConnection;

            AsyncCallback<SqlConnection> setConnectionAndPush = createAsyncCallback(ctn -> {
                pull(in);
                currentConnection = ctn;
                first = false;
            });
            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        T grab = grab(in);
                        push(out, grab);
                        if(onEach) {
                            currentConnection.connection().commit();
                        }
                    }

                    @Override
                    public void onUpstreamFinish() throws Exception {
                            if(!onEach)
                                currentConnection.connection().commit();
                            complete(out);
                    }

                    @Override
                    public void onUpstreamFailure(Throwable ex) throws Exception {
                        currentConnection.connection().rollback();
                    }
                });

                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onPull() throws Exception {
                        if(first) {
                            sqlConnection.thenAccept(setConnectionAndPush::invoke);
                        } else {
                            pull(in);
                        }
                    }
                });
            }
        };
    }
}
