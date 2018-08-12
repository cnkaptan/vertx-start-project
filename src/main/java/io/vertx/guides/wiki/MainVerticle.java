package io.vertx.guides.wiki;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;

import java.util.Date;
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

    private static final String EMPTY_PAGE_MARKDOWN =
            "# A new page\n" +
                    "\n" +
                    "Feel-free to write in Markdown!\n";

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
        router.get("/wiki/:page").handler(this::pageRenderingHandler);
        /**
         * This makes all HTTP POST requests go through a first handler, here
         * io.vertx.ext.web.handler.BodyHandler. This handler automatically decodes the body from the
         * HTTP requests (e.g., form submissions), which can then be manipulated as Vert.x buffer objects.
         */
        router.post().handler(BodyHandler.create());
        router.post("/save").handler(this::pageUpdateHandler);
        router.post("/crete").handler(this::pageCreateHandler);
        router.post("/delete").handler(this::pageDeletionHandler);

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

    private void pageRenderingHandler(RoutingContext context) {
        // URL parameters (/wiki/:page here) can be accessed through the context request object.
        String page = context.request().getParam("page");

        dbClient.getConnection(car -> {
            if (car.succeeded()) {

                SQLConnection connection = car.result();
                // Passing argument values to SQL queries is done using a JsonArray, with the elements in order of
                // the ? symbols in the SQL query.
                connection.queryWithParams(SQL_GET_PAGE, new JsonArray().add(page), fetch -> {
                    connection.close();

                    if (fetch.succeeded()) {

                        JsonArray row = fetch.result().getResults()
                                .stream()
                                .findFirst()
                                .orElseGet(() -> new JsonArray().add(-1).add(EMPTY_PAGE_MARKDOWN));

                        Integer id = row.getInteger(0);
                        String rawContent = row.getString(1);

                        context.put("title", page);
                        context.put("id", id);
                        context.put("newPage", fetch.result().getResults().size() == 0 ? "yes" : "no");
                        context.put("rawContent", rawContent);
                        // The Processor class comes from the txtmark Markdown rendering library that we use.
                        context.put("content", Processor.process(rawContent));
                        context.put("timestamp", new Date().toString());

                        templateEngine.render(context, "templates", "/page.ftl", ar -> {
                            if (ar.succeeded()) {
                                context.response().putHeader("Content-Type", "text/html");
                                context.response().end(ar.result());
                            } else {
                                context.fail(ar.cause());
                            }
                        });
                    } else {
                        context.fail(fetch.cause());
                    }
                });
            } else {
                context.fail(car.cause());
            }
        });
    }

    private void pageCreateHandler(RoutingContext context) {
        String pageName = context.request().getParam("name");
        String location = "/wiki/" + pageName;

        if (pageName == null || pageName.isEmpty()) {
            location = "/";
        }

        context.response().setStatusCode(303);
        context.response().putHeader("Location", location);
        context.response().end();
    }


    private void pageUpdateHandler(RoutingContext context) {
        /**
         * Form parameters sent through a HTTP POST request are available from the RoutingContext
         * object. Note that without a BodyHandler within the Router configuration chain these values would
         * not be available, and the form submission payload would need to be manually decoded from the
         * HTTP POST request payload
         */
        String id = context.request().getParam("id");
        String title = context.request().getParam("title");
        String markdown = context.request().getParam("markdown");

        /**
         * We rely on a hidden form field rendered in the page.ftl FreeMarker template to know if we are
         * updating an existing page or saving a new page.
         */
        boolean newPage = "yes".equals(context.request().getParam("newPage"));

        dbClient.getConnection(car -> {
            if (car.succeeded()) {
                SQLConnection connection = car.result();
                String sql = newPage ? SQL_CREATE_PAGE : SQL_SAVE_PAGE;

                // Again, preparing the SQL query with parameters uses a JsonArray to pass values.
                JsonArray params = new JsonArray();
                if (newPage) {
                    params.add(title).add(markdown);
                } else {
                    params.add(markdown).add(id);
                }

                // The updateWithParams method is used for insert / update / delete SQL queries.
                connection.updateWithParams(sql, params, res -> {
                    connection.close();
                    if (res.succeeded()) {
                        // Upon success, we simply redirect to the page that has been edited.
                        context.response().setStatusCode(303);
                        context.response().putHeader("Location", "/wiki/" + title);
                        context.response().end();
                    } else {
                        context.fail(res.cause());
                    }
                });
            } else {
                context.fail(car.cause());
            }
        });
    }

    private void pageDeletionHandler(RoutingContext context) {
        String id = context.request().getParam("id");
        dbClient.getConnection(car -> {
            if (car.succeeded()) {
                SQLConnection connection = car.result();
                connection.updateWithParams(SQL_DELETE_PAGE, new JsonArray().add(id), res -> {
                    connection.close();
                    if (res.succeeded()) {
                        context.response().setStatusCode(303);
                        context.response().putHeader("Location", "/");
                        context.response().end();
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
