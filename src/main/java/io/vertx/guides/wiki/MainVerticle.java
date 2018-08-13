package io.vertx.guides.wiki;

import io.vertx.core.*;

public class MainVerticle extends AbstractVerticle {

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        /**
         * When the future of prepareDatabase completes successfully, then startHttpServer is called and the
         * steps future completes depending of the outcome of the future returned by startHttpServer.
         * startHttpServer is never called if prepareDatabase encounters an error, in which case the steps
         * future is in a failed state and becomes completed with the exception describing the error.
         */
//        Future<Void> steps = prepeareDatabase().compose(v -> startHttpServer());
//        steps.setHandler(ar -> {
//            if (ar.succeeded()) {
//                startFuture.completer();
//            } else {
//                startFuture.fail(ar.cause());
//            }
//        });

        /**
         * We still have a MainVerticle class, but instead of containing all the business logic like in the initial
         * iteration, its sole purpose is to bootstrap the application and deploy other verticles.
         *
         * The code consists in deploying 1 instance of WikiDatabaseVerticle and 2 instances of
         * HttpServerVerticle :
         */

        // Deploying a verticle is an asynchronous operation, so we need a Future for that. The String
        // parametric type is because a verticle gets an identifier when successfully deployed
        Future<String> dbVerticleDeployment = Future.future();

        // One option is to create a verticle instance with new, and pass the object reference to the deploy
        // method. The completer return value is a handler that simply completes its future.
        vertx.deployVerticle(new WikiDatabaseVerticle(), dbVerticleDeployment.completer());

        // Sequential composition with compose allows to run one asynchronous operation after the other.
        // When the initial future completes successfully, the composition function is invoked.
        dbVerticleDeployment.compose(id -> {
            Future<String> httpVerticalDeployment = Future.future();
            // A class name as a string is also an option to specify a verticle to deploy. For other JVM languages
            // string-based conventions allow a module / script to be specified.
            vertx.deployVerticle(
                    "io.vertx.guides.wiki.HttpServerVerticle",
                    // The DeploymentOption class allows to specify a number of parameters and especially the number
                    // of instances to deploy.
                    new DeploymentOptions().setInstances(2),
                    httpVerticalDeployment.completer());

            // The composition function returns the next future. Its completion will trigger the completion of
            // the composite operation.
            return httpVerticalDeployment;
        }).setHandler(ar -> {
            // We define a handler that eventually completes the MainVerticle start future.
            if (ar.succeeded()) {
                startFuture.complete();
            } else {
                startFuture.fail(ar.cause());
            }
        });
    }
}
