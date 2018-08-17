package io.vertx.guides.wiki.database;

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;

import java.util.HashMap;
import java.util.List;

/**
 * That’s the main purpose of service proxies. It lets you expose a service on the event bus,
 * so, any other Vert.x component can consume it,
 * as soon as they know the address on which the service is published.
 *
 * A service is described with a Java interface containing methods following the async pattern.
 * Under the hood, messages are sent on the event bus to invoke the service and get the response back.
 * But for ease of use, it generates a proxy that you can invoke directly (using the API from the service interface).
 *
 *
 * Defining a service interface is as simple as defining a Java interface, except that there are certain
 * rules to respect for code generation to work and also to ensure inter-operability with other code in
 * Vert.x.
 *
 * The ProxyGen annotation is used to trigger the code generation of a proxy for clients of that
 * service.
 */
@ProxyGen
public interface WikiDatabaseService {

    /**
     * The Fluent annotation is optional, but allows fluent interfaces where operations can be chained
     * by returning the service instance. This is mostly useful for the code generator when the service
     * shall be consumed from other JVM languages
     * @param resultHandler
     * @return
     */
    @Fluent
    WikiDatabaseService fetchAllPages(Handler<AsyncResult<JsonArray>> resultHandler);

    @Fluent
    WikiDatabaseService fetchPage(String name, Handler<AsyncResult<JsonObject>> resultHandler);

    /**
     * Parameter types need to be strings, Java primitive types, JSON objects or arrays, any
     * enumeration type or a java.util collection (List / Set / Map) of the previous types. The only way
     * to support arbitrary Java classes is to have them as Vert.x data objects, annotated with
     * @DataObject. The last opportunity to pass other types is service reference types
     * @param title
     * @param markdown
     * @param resultHandler
     * @return
     */
    @Fluent
    WikiDatabaseService createPage(String title, String markdown, Handler<AsyncResult<Void>> resultHandler);

    /**
     * Since services provide asynchronous results, the last argument of a service method needs to be
     * a Handler<AsyncResult<T>> where T is any of the types suitable for code generation as described
     * above.
     * @param id
     * @param markdown
     * @param resultHandler
     * @return
     */
    @Fluent
    WikiDatabaseService savePage(int id, String markdown, Handler<AsyncResult<Void>> resultHandler);

    @Fluent
    WikiDatabaseService deletePage(int id, Handler<AsyncResult<Void>> resultHandler);

    @Fluent
    WikiDatabaseService fetchAllPagesData(Handler<AsyncResult<List<JsonObject>>> resultHandler);


    /**
     * It is a good practice that service interfaces provide static methods to create instances of both the
     * actual service implementation and proxy for client code over the event bus.
     * @param dbClient
     * @param sqlQueries
     * @param readyHandler
     * @return
     */
    static WikiDatabaseService create(JDBCClient dbClient,
                                      HashMap<SqlQuery, String> sqlQueries,
                                      Handler<AsyncResult<WikiDatabaseService>> readyHandler){
        return new WikiDatabaseServiceImpl(dbClient, sqlQueries, readyHandler);
    }

    /**
     * The Vert.x code generator creates the proxy class and names it by suffixing with VertxEBProxy.
     * Constructors of these proxy classes need a reference to the Vert.x context as well as a destination
     * address on the event bus:
     *
     * The WikiDatabaseServiceVertxEBProxy generated class handles receiving messages on the event bus
     * and then dispatching them to the WikiDatabaseServiceImpl. What it does is actually very close to
     * what we did in the previous section: messages are being sent with a action header to specify which
     * method to invoke, and parameters are encoded in JSON.
     *
     * @param vertx
     * @param address
     * @return
     */
    static  WikiDatabaseService createProxy(Vertx vertx, String address){
        return new WikiDatabaseServiceVertxEBProxy(vertx,address);
    }

}
