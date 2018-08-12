package io.vertx.guides.wiki;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;

import java.util.List;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {

    /**
     * The database operations will be typical create, read, update, delete operations. To get us started, we
     * simply store the corresponding SQL queries as static fields of the MainVerticle class. Note that they
     * are written in a SQL dialect that HSQLDB understands, but that other relational databases may not
     * necessarily support:
     */
    private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob)";
    private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
    private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
    private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
    private static final String SQL_ALL_PAGES = "select Name from Pages";
    private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";

    private JDBCClient dbClient;
    private Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

    private final FreeMarkerTemplateEngine templateEngine = FreeMarkerTemplateEngine.create();


    @Override
    public void start(Future<Void> startFuture) throws Exception {
        /**
         * When the future of prepareDatabase completes successfully, then startHttpServer is called and the
         * steps future completes depending of the outcome of the future returned by startHttpServer.
         * startHttpServer is never called if prepareDatabase encounters an error, in which case the steps
         * future is in a failed state and becomes completed with the exception describing the error.
         */
        Future<Void> steps = prepeareDatabase().compose(v -> startHttpServer());
        steps.setHandler(ar -> {
            if (ar.succeeded()) {
                startFuture.completer();
            } else {
                startFuture.fail(ar.cause());
            }
        });

    }


    /**
     * It attempts to obtain a JDBC client connection, then performs a SQL query to create the Pages table unless it
     * already exists
     *
     * @return
     */
    private Future<Void> prepeareDatabase() {
        Future<Void> future = Future.future();
        dbClient = JDBCClient.createNonShared(vertx, new JsonObject()
                .put("url", "jdbc:hsqldb:file:db/wiki")
                .put("driver_class", "org.hsqldb.jdbcDriver")
                .put("max_pool_size", 30));

        dbClient.getConnection(ar -> {
            if (ar.failed()) {
                LOGGER.error("Could not open a database connection", ar.cause());
                future.fail(ar.cause());
            } else {
                SQLConnection connection = ar.result();
                connection.execute(SQL_CREATE_PAGES_TABLE, create -> {
                    connection.close();
                    if (create.failed()) {
                        future.fail(create.cause());
                        LOGGER.error("Database preparation error = " + create.cause().getMessage(), create.cause());
                    } else {
                        future.complete();
                    }
                });
            }
        });
        return future;
    }

    private Future<Void> startHttpServer() {
        Future<Void> future = Future.future();
        HttpServer server = vertx.createHttpServer();

        Router router = Router.router(vertx);
        router.get("/alive").handler(ro -> ro.response().end("Alive"));
        router.get("/").handler(this::indexHandler);
//        router.get("/wiki/:page").handler(this::pageRenderingHandler);
        /**
         * This makes all HTTP POST requests go through a first handler, here
         * io.vertx.ext.web.handler.BodyHandler. This handler automatically decodes the body from the
         * HTTP requests (e.g., form submissions), which can then be manipulated as Vert.x buffer objects.
         */
        router.post().handler(BodyHandler.create());
//        router.post("/save").handler(this::pageUpdateHandler);
//        router.post("/crete").handler(this::pageCreateHandler);
//        router.post("/delete").handler(this::pageDeletionHandler);

        /**
         * The router object can be used as a HTTP server handler, which then dispatches to other
         * handlers as defined above.
         */
        server.requestHandler(router::accept)
                /**
                 * Starting a HTTP server is an asynchronous operation, so an AsyncResult<HttpServer> needs to be
                 * checked for success. By the way the 8080 parameter specifies the TCP port to be used by the
                 * server.
                 */
                .listen(8080, ar -> {
                    if (ar.succeeded()) {
                        LOGGER.info("HTTP server running on port 8080");
                        future.complete();
                    } else {
                        LOGGER.error("Could not start a HTTP server", ar.cause());
                        future.fail(ar.cause());
                    }
                });
        return future;
    }

    private void indexHandler(RoutingContext context) {
        dbClient.getConnection(car -> {
            if (car.succeeded()) {
                SQLConnection connection = car.result();
                connection.query(SQL_ALL_PAGES, res -> {
                    connection.close();

                    if (res.succeeded()) {
                        // SQL query results are being returned as instances of JsonArray and JsonObject.
                        List<String> pages = res.result()
                                .getResults()
                                .stream()
                                .map(json -> json.getString(0))
                                .sorted()
                                .collect(Collectors.toList());

                        /**
                         * The RoutingContext instance can be used to put arbitrary key / value data that is then available
                         * from templates, or chained router handlers.
                         */
                        context.put("title", "Wiki home");
                        context.put("pages", pages);
                        templateEngine.render(context, "templates", "/index.ftl", ar -> {
                            if (ar.succeeded()) {
                                context.response().putHeader("Content-Type", "text/html");
                                /**
                                 * The AsyncResult contains the template rendering as a String in case of success, and we can end
                                 * the HTTP response stream with the value.
                                 */
                                context.response().end(ar.result());
                            } else {
                                context.fail(ar.cause());
                            }
                        });
                    } else {
                        context.fail(res.cause());
                    }
                });
            } else {
                context.fail(car.cause());
            }
        });
    }
}
