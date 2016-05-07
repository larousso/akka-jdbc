package com.adelegue.akka.jdbc.utils;

import akka.actor.ActorSystem;
import akka.dispatch.OnFailure;
import akka.dispatch.OnSuccess;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Created by adelegue on 30/04/2016.
 */
public class AkkaUtils {

    public static ExecutionContext getExecutionContext(ActorSystem actorSystem, Optional<String> dispatcher) {
        Optional<ExecutionContext> messageDispatcher = dispatcher.map(actorSystem.dispatchers()::lookup);
        return messageDispatcher.orElse(actorSystem.dispatcher());
    }

    public static <T> CompletableFuture<T> toJavaFuture(Future<T> source, ExecutionContext executionContext) {
        CompletableFuture<T> javaFuture = new CompletableFuture<>();
        source.onFailure(new OnFailure() {
            @Override
            public void onFailure(Throwable failure) throws Throwable {
                javaFuture.completeExceptionally(failure);
            }
        }, executionContext);

        source.onSuccess(new OnSuccess<T>() {
            @Override
            public void onSuccess(T result) throws Throwable {
                javaFuture.complete(result);
            }
        }, executionContext);

        return javaFuture;
    }
}
