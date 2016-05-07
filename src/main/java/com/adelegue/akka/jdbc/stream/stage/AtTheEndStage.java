package com.adelegue.akka.jdbc.stream.stage;

import akka.japi.Procedure;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Inlet;
import akka.stream.Outlet;
import akka.stream.stage.*;
import com.adelegue.akka.jdbc.connection.SqlConnection;
import com.adelegue.akka.jdbc.stream.ResourcesHelper;
import scala.compat.java8.FutureConverters;
import scala.concurrent.Future;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Created by adelegue on 30/04/2016.
 */
public class AtTheEndStage<T> extends GraphStage<FlowShape<T, T>> implements ResourcesHelper {

    final Inlet<T> in = Inlet.create("QueryStage.in");
    final Outlet<T> out = Outlet.create("QueryStage.out");

    private final CompletionStage<SqlConnection> sqlConnection;

    private final List<Procedure<SqlConnection>> actions;

    public AtTheEndStage(List<Procedure<SqlConnection>> actions, Future<SqlConnection> sqlConnection) {
        this.sqlConnection = FutureConverters.toJava(sqlConnection);
        this.actions = actions;
    }

    private final FlowShape<T, T> shape = FlowShape.of(in, out);
    @Override
    public FlowShape<T, T> shape() {
        return shape;
    }

    @Override
    public GraphStageLogic createLogic(Attributes inheritedAttributes) {
        return new GraphStageLogic(shape) {

            Boolean first = true;
            SqlConnection currentConnection;

            AsyncCallback<SqlConnection> setConnectionAndPush = createAsyncCallback(ctn -> {
                pull(in);
                currentConnection = ctn;
                first = false;
            });

            private void apply() {
                if(currentConnection != null) {
                    Boolean ok = actions.stream().map(action -> {
                        try {
                            action.apply(currentConnection);
                            return true;
                        } catch (Exception e) {
                            return false;
                        }
                    }).reduce(true, (acc, el) -> acc && el);
                    if(!ok) {
                        currentConnection.connection();
                    }
                }
            }

            {
                setHandler(in, new AbstractInHandler() {
                    @Override
                    public void onPush() throws Exception {
                        push(out, grab(in));
                    }

                    @Override
                    public void onUpstreamFinish() throws Exception {
                        apply();
                        complete(out);
                    }

                    @Override
                    public void onUpstreamFailure(Throwable ex) throws Exception {
                        if(currentConnection != null) {
                            currentConnection.connection().close();
                        }
                        fail(out, ex);
                    }
                });

                setHandler(out, new AbstractOutHandler() {
                    @Override
                    public void onDownstreamFinish() throws Exception {
                        apply();
                    }

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
