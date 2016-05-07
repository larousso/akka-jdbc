package com.adelegue.akka.jdbc.utils;

import akka.actor.ActorSystem;
import akka.stream.ActorAttributes;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Source;
import scala.concurrent.ExecutionContext;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Created by adelegue on 30/04/2016.
 */
public class AkkaUtils {

    public static ExecutionContext getExecutionContext(ActorSystem actorSystem, Optional<String> dispatcher) {
        Optional<ExecutionContext> messageDispatcher = dispatcher.map(actorSystem.dispatchers()::lookup);
        return messageDispatcher.orElse(actorSystem.dispatcher());
    }

    public static <In, Out, Mat> Flow<In, Out, Mat> applyDispatcher(Flow<In, Out, Mat> aFlow, String dispatcher) {
        return aFlow.withAttributes(ActorAttributes.dispatcher(dispatcher));
    }

    public static <Out, Mat> Source<Out, Mat> applyDispatcher(Source<Out, Mat> aSource, String dispatcher) {
        return aSource.withAttributes(ActorAttributes.dispatcher(dispatcher));
    }

}
