package com.adelegue.akka.jdbc.utils;

import akka.actor.ActorSystem;
import scala.concurrent.ExecutionContext;

import java.util.Optional;

/**
 * Created by adelegue on 30/04/2016.
 */
public class AkkaUtils {

    public static ExecutionContext getExecutionContext(ActorSystem actorSystem, Optional<String> dispatcher) {
        Optional<ExecutionContext> messageDispatcher = dispatcher.map(actorSystem.dispatchers()::lookup);
        return messageDispatcher.orElse(actorSystem.dispatcher());
    }

}
