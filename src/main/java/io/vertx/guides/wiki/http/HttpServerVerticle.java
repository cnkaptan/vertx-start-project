package io.vertx.guides.wiki.http;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.ext.web.codec.BodyCodec;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;
import io.vertx.guides.wiki.database.WikiDatabaseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class HttpServerVerticle extends AbstractVerticle {
    public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
    public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);

    private final FreeMarkerTemplateEngine templateEngine = FreeMarkerTemplateEngine.create();

    private static final String EMPTY_PAGE_MARKDOWN =
            "# A new page\n" +
                    "\n" +
                    "Feel-free to write in Markdown!\n";

    // tag::db-consume[]
    private WikiDatabaseService dbService;
    private WebClient webClient;

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        String wikiDbQueue = config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue"); // <1>
        dbService = WikiDatabaseService.createProxy(vertx, wikiDbQueue);

        /**
         * Since requests are made using HTTPS, we need to configure the web client with
         * SSL support.
         *
         * We override the default user agent with vert-x3 but you may opt to use your own
         * value instead.
         */
        webClient = WebClient.create(vertx, new WebClientOptions()
                .setSsl(true)
                .setUserAgent("vert-x3"));


        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
        router.get("/").handler(this::indexHandler);
        router.get("/alive").handler(context -> context.response().end("Alive"));
        router.get("/wiki/:page").handler(this::pageRenderingHandler);
        router.get("/backup").handler(this::backupHandler);

        router.post().handler(BodyHandler.create());
        router.post("/save").handler(this::pageUpdateHandler);
        router.post("/create").handler(this::pageCreateHandler);
        router.post("/delete").handler(this::pageDeletionHandler);

        int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
        server
                .requestHandler(router::accept)
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

    private void backupHandler(RoutingContext context) {
        dbService.fetchAllPagesData(reply -> {
            if (reply.succeeded()) {
                JsonArray filesObject = new JsonArray();
                JsonObject payload = new JsonObject()
                        .put("files", filesObject)
                        .put("language", "plaintext")
                        .put("title", "vertx-wiki-backup")
                        .put("public", "true");

                reply.result()
                        .forEach(page ->{
                            /**
                             * Each file is an entry under the files object of the payload, with a title and content.
                             */
                            JsonObject fileObject = new JsonObject();
                            fileObject.put("name", page.getString("NAME"));
                            fileObject.put("content", page.getString("CONTENT"));
                            filesObject.add(fileObject);
                        });

                /**
                 * The web client needs to issue a POST request on port 443 (HTTPS), and the path must be
                 * /snippets.
                 *
                 * The BodyCodec class provides a helper to specify that the response will be directly converted to a
                 * Vert.x JsonObject instance. It is also possible to use BodyCodec#json(Class<T>) and the JSON data
                 * will be mapped to a Java object of type T (this uses Jackson data mapping under the hood).
                 *
                 * sendJsonObject is a helper for triggering the HTTP request with a JSON payload.
                 *
                 * Upon success we can get the snippet identifier, and construct a URL to the user-friendly web
                 * representation.
                 */
                webClient.post(443, "snippets.glot.io", "/snippets")
                        .putHeader("Content-Type", "application/json")
                        .as(BodyCodec.jsonObject())
                        .sendJsonObject(payload, ar -> {
                            if (ar.succeeded()) {
                                HttpResponse<JsonObject> response = ar.result();
                                if (response.statusCode() == 200) {
                                    String url = "https://glot.io/snippets/" + response.body().getString("id");
                                    context.put("backup_gist_url", url);
                                    indexHandler(context);
                                } else {
                                    StringBuilder message = new StringBuilder()
                                            .append("Could not backup the wiki: ")
                                            .append(response.statusMessage());

                                    JsonObject body = response.body();
                                    if (body != null) {
                                        message.append(System.getProperty("line.separator"))
                                                .append(body.encodePrettily());
                                        LOGGER.error(message.toString());
                                        context.fail(502);
                                    }
                                }
                            } else {
                                Throwable err = ar.cause();
                                LOGGER.error("HTTP CLIENT ERROR", err);
                                context.fail(err);
                            }
                        });
            } else {
                context.fail(reply.cause());
            }
        });
    }

    // tag::db-service-calls[]
    private void indexHandler(RoutingContext context) {
        dbService.fetchAllPages(reply -> {
            if (reply.succeeded()) {
                context.put("title", "Wiki home");
                context.put("pages", reply.result().getList());
                templateEngine.render(context, "templates", "/index.ftl", ar -> {
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

    private void pageRenderingHandler(RoutingContext context) {
        String requestedPage = context.request().getParam("page");
        dbService.fetchPage(requestedPage, reply -> {
            if (reply.succeeded()) {

                JsonObject payLoad = reply.result();
                boolean found = payLoad.getBoolean("found");
                String rawContent = payLoad.getString("rawContent", EMPTY_PAGE_MARKDOWN);
                context.put("title", requestedPage);
                context.put("id", payLoad.getInteger("id", -1));
                context.put("newPage", found ? "no" : "yes");
                context.put("rawContent", rawContent);
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

    private void pageUpdateHandler(RoutingContext context) {
        String title = context.request().getParam("title");

        Handler<AsyncResult<Void>> handler = reply -> {
            if (reply.succeeded()) {
                context.response().setStatusCode(303);
                context.response().putHeader("Location", "/wiki/" + title);
                context.response().end();
            } else {
                context.fail(reply.cause());
            }
        };

        String markdown = context.request().getParam("markdown");
        if ("yes".equals(context.request().getParam("newPage"))) {
            dbService.createPage(title, markdown, handler);
        } else {
            dbService.savePage(Integer.valueOf(context.request().getParam("id")), markdown, handler);
        }
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

    private void pageDeletionHandler(RoutingContext context) {
        dbService.deletePage(Integer.valueOf(context.request().getParam("id")), reply -> {
            if (reply.succeeded()) {
                context.response().setStatusCode(303);
                context.response().putHeader("Location", "/");
                context.response().end();
            } else {
                context.fail(reply.cause());
            }
        });
    }
    // end::db-service-calls[]
}
