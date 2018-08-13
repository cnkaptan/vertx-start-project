package io.vertx.guides.wiki;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class HttpServerVerticle extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

    /**
     * We expose public constants for the verticle configuration parameters: the HTTP port number
     * and the name of the event bus destination to post messages to the database verticle.
     */
    public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
    public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

    private String wikiDbQueque = "wikidb.queue";

    private final FreeMarkerTemplateEngine templateEngine = FreeMarkerTemplateEngine.create();

    private static final String EMPTY_PAGE_MARKDOWN =
            "# A new page\n" +
                    "\n" +
                    "Feel-free to write in Markdown!\n";

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        /**
         * The AbstractVerticle#config() method allows accessing the verticle configuration that has been
         * provided. The second parameter is a default value in case no specific value was given.
         */
        wikiDbQueque = config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue");

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
        router.post("/create").handler(this::pageCreateHandler);
        router.post("/delete").handler(this::pageDeletionHandler);

        /**
         * Configuration values can not just be String objects but also integers, boolean values, complex JSON data, etc.
         */
        int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);

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
                .listen(portNumber, ar -> {
                    if (ar.succeeded()) {
                        LOGGER.info("HTTP server running on port " + portNumber);
                        startFuture.complete();
                    } else {
                        LOGGER.error("Could not start a HTTP server", ar.cause());
                        startFuture.fail(ar.cause());
                    }
                });

    }

    public void indexHandler(RoutingContext context) {
        // Delivery options allow us to specify headers, payload codecs and timeouts.
        // We encode payloads as JSON objects, and we specify which action the database verticle should do
        //through a message header called action.
        DeliveryOptions options = new DeliveryOptions().addHeader("action", "all-pages");

        /**
         * The vertx object gives access to the event bus, and we send a message to the queue for the
         * database verticle
         */
        vertx.eventBus().send(wikiDbQueque, new JsonObject(), options, reply -> {
            if (reply.succeeded()) {
                // Upon success a reply contains a payload.
                JsonObject body = (JsonObject) reply.result().body();


                /**
                 * The RoutingContext instance can be used to put arbitrary key / value data that is then available
                 * from templates, or chained router handlers.
                 */
                context.put("title", "Wiki home");
                context.put("pages", body.getJsonArray("pages").getList());
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
                context.fail(reply.cause());
            }
        });
    }

    public void pageRenderingHandler(RoutingContext context) {
        String requestedPage = context.request().getParam("page");
        JsonObject request = new JsonObject().put("page", requestedPage);

        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "get-page");
        vertx.eventBus().send(wikiDbQueque, request, deliveryOptions, reply -> {
            if (reply.succeeded()) {
                JsonObject body = (JsonObject) reply.result().body();

                boolean found = body.getBoolean("found");
                String rawContent = body.getString("rawContent", EMPTY_PAGE_MARKDOWN);
                context.put("title", requestedPage);
                context.put("id", body.getInteger("id", -1));
                context.put("newPage", found ? "no" : "yes");
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
                context.fail(reply.cause());
            }
        });
    }

    public void pageUpdateHandler(RoutingContext context) {
        /**
         * Form parameters sent through a HTTP POST request are available from the RoutingContext
         * object. Note that without a BodyHandler within the Router configuration chain these values would
         * not be available, and the form submission payload would need to be manually decoded from the
         * HTTP POST request payload
         */
        String title = context.request().getParam("title");
        JsonObject request = new JsonObject()
                .put("id", context.request().getParam("id"))
                .put("title", title)
                .put("markdown", context.request().getParam("markdown"));

        DeliveryOptions options = new DeliveryOptions();

        /**
         * We rely on a hidden form field rendered in the page.ftl FreeMarker template to know if we are
         * updating an existing page or saving a new page.
         */
        if ("yes".equals(context.request().getParam("newPage"))) {
            options.addHeader("action", "create-page");
        } else {
            options.addHeader("action", "save-page");
        }

        vertx.eventBus().send(wikiDbQueque, request, options, reply -> {
            if (reply.succeeded()) {
                // Upon success, we simply redirect to the page that has been edited.
                context.response().setStatusCode(303);
                context.response().putHeader("Location", "/wiki/" + title);
                context.response().end();
            } else {
                context.fail(reply.cause());
            }
        });
    }

    public void pageCreateHandler(RoutingContext context) {
        String pageName = context.request().getParam("name");
        String location = "/wiki/" + pageName;
        if (pageName == null || pageName.isEmpty()) {
            location = "/";
        }

        context.response().setStatusCode(303);
        context.response().putHeader("Location", location);
        context.response().end();
    }

    public void pageDeletionHandler(RoutingContext context) {
        String id = context.request().getParam("id");
        JsonObject request = new JsonObject()
                .put("id", id);

        DeliveryOptions options = new DeliveryOptions().addHeader("action", "delete-page");
        vertx.eventBus().send(wikiDbQueque, request, options, reply -> {
            if (reply.succeeded()) {
                context.response().setStatusCode(303);
                context.response().putHeader("Location", "/");
                context.response().end();
            } else {
                context.fail(reply.cause());
            }
        });
    }


}
