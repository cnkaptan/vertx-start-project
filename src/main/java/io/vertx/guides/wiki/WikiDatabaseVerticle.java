package io.vertx.guides.wiki;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class WikiDatabaseVerticle extends AbstractVerticle {
    public static final String CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url";
    public static final String CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class";
    public static final String CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size";
    public static final String CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueries.resource.file";
    public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

    private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseVerticle.class);

    /**
     * The following code from the WikiDatabaseVerticle class loads the SQL queries from a file, and make
     * them available from a map:
     */
    private enum SqlQuery {
        CREATE_PAGES_TABLE,
        ALL_PAGES,
        GET_PAGE,
        CREATE_PAGE,
        SAVE_PAGE,
        DELETE_PAGE
    }

    // We defined a ErrorCodes enumeration for errors, which we use to report back to the message sender.
    private enum ErrorCodes {
        NO_ACTION_SPECIFIED,
        BAD_ACTION,
        DB_ERROR
    }

    private final HashMap<SqlQuery, String> sqlQueries = new HashMap<>();

    private JDBCClient dbClient;

    /**
     * It attempts to obtain a JDBC client connection, then performs a SQL query to create the Pages table unless it
     * already exists
     *
     * @return
     */
    @Override
    public void start(Future<Void> startFuture) throws Exception {
        /*
         * Note: this uses blocking APIs, but data is small...
         * Interestingly we break an important principle in Vert.x which is to avoid blocking APIs, but
         * since there are no asynchronous APIs for accessing resources on the classpath our options are
         * limited. We could use the Vert.x executeBlocking method to offload the blocking I/O operations
         * from the event loop to a worker thread, but since the data is very small there is no obvious
         * benefit in doing so.
         */
        loadSqlQueries();

        dbClient = JDBCClient.createShared(vertx, new JsonObject()
                .put("url", config().getString(CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:file:db/wiki"))
                .put("driver_class", config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver"))
                .put("max_pool_size", config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 30)));

        dbClient.getConnection(ar -> {
            if (ar.failed()) {
                LOGGER.error("Could not open a database connection", ar.cause());
                startFuture.fail(ar.cause());
            } else {
                SQLConnection connection = ar.result();

                // Here is an example of using SQL queries.
                connection.execute(sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE), create -> {
                    connection.close();
                    if (create.failed()) {
                        LOGGER.error("Database preparation error", create.cause());
                        startFuture.fail(create.cause());
                    } else {
                        // The consumer method registers an event bus destination handler.
                        vertx.eventBus().consumer(config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue"),
                                this::onMessage);
                        startFuture.complete();
                    }
                });
            }
        });

    }


    private void loadSqlQueries() throws IOException {
        String queriesFile = config().getString(CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE);
        InputStream queriesInputStream;

        if (queriesFile != null) {
            queriesInputStream = new FileInputStream(queriesFile);
        } else {
            queriesInputStream = getClass().getResourceAsStream("/db-queries.properties");
        }

        Properties queriesProps = new Properties();
        queriesProps.load(queriesInputStream);
        queriesInputStream.close();

        // We use the SqlQuery enumeration type to avoid string constants later in the code.
        sqlQueries.put(SqlQuery.CREATE_PAGES_TABLE, queriesProps.getProperty("create-pages-table"));
        sqlQueries.put(SqlQuery.ALL_PAGES, queriesProps.getProperty("all-pages"));
        sqlQueries.put(SqlQuery.GET_PAGE, queriesProps.getProperty("get-page"));
        sqlQueries.put(SqlQuery.CREATE_PAGE, queriesProps.getProperty("create-page"));
        sqlQueries.put(SqlQuery.SAVE_PAGE, queriesProps.getProperty("save-page"));
        sqlQueries.put(SqlQuery.DELETE_PAGE, queriesProps.getProperty("delete-page"));
    }


    public void onMessage(Message<JsonObject> message) {
        if (!message.headers().contains("action")) {
            LOGGER.error("No action header specified for message with headers {} and body {}",
                    message.headers(), message.body().encodePrettily());

            // the fail method of the Message class provides a convenient shortcut to reply with
            // an error, and the original message sender gets a failed AsyncResult
            message.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action header specified");
            return;
        }

        String action = message.headers().get("action");

        switch (action) {
            case "all-pages":
                fetchAllPages(message);
                break;
            case "get-page":
                fetchPage(message);
                break;
            case "create-page":
                createPage(message);
                break;
            case "save-page":
                savePage(message);
                break;
            case "delete-page":
                deletePage(message);
                break;
            default:
                message.fail(ErrorCodes.BAD_ACTION.ordinal(), "Bad action: " + action);
                break;
        }
    }

    private void fetchAllPages(Message<JsonObject> message) {
        LOGGER.info("fetchAllPages",
                message.headers(), message.body().encodePrettily());

        dbClient.query(sqlQueries.get(SqlQuery.ALL_PAGES), res -> {
            if (res.succeeded()) {
                // SQL query results are being returned as instances of JsonArray and JsonObject.
                List<String> pages = res.result()
                        .getResults()
                        .stream()
                        .map(json -> json.getString(0))
                        .sorted()
                        .collect(Collectors.toList());
                message.reply(new JsonObject().put("pages", new JsonArray(pages)));
            } else {
                reportQueryError(message, res.cause());
            }
        });
    }

    private void fetchPage(Message<JsonObject> message) {
        LOGGER.info("fetchPage",
                message.headers(), message.body().encodePrettily());
        // URL parameters (/wiki/:page here) can be accessed through the context request object.
        String requestedPage = message.body().getString("page");
        JsonArray params = new JsonArray().add(requestedPage);

        // Passing argument values to SQL queries is done using a JsonArray, with the elements in order of
        // the ? symbols in the SQL query.
        dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_PAGE), params, fetch -> {
            if (fetch.succeeded()) {
                JsonObject response = new JsonObject();
                ResultSet resultSet = fetch.result();

                if (resultSet.getNumRows() == 0) {
                    response.put("found", false);
                } else {
                    response.put("found", true);
                    JsonArray row = resultSet.getResults().get(0);
                    response.put("id", row.getInteger(0));
                    response.put("rawContent", row.getString(1));
                }
                message.reply(response);
            } else {
                reportQueryError(message, fetch.cause());
            }
        });
    }

    private void createPage(Message<JsonObject> message) {
        LOGGER.info("createPage",
                message.headers(), message.body().encodePrettily());
        JsonObject request = message.body();
        JsonArray data = new JsonArray();
        data.add(request.getString("title"));
        data.add(request.getString("markdown"));

        dbClient.queryWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), data, res -> {
            if (res.succeeded()) {
                message.reply("ok");
            } else {
                reportQueryError(message, res.cause());
            }
        });
    }

    private void savePage(Message<JsonObject> message) {
        LOGGER.info("savePage",
                message.headers(), message.body().encodePrettily());
        JsonObject request = message.body();
        // Again, preparing the SQL query with parameters uses a JsonArray to pass values.
        JsonArray data = new JsonArray();
        data.add(request.getString("markdown"))
                .add(request.getString("id"));

        // The updateWithParams method is used for insert / update / delete SQL queries.
        dbClient.updateWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), data, res -> {
            if (res.succeeded()) {
                message.reply("ok");
            } else {
                reportQueryError(message, res.cause());
            }
        });
    }

    private void deletePage(Message<JsonObject> message) {
        LOGGER.info("deletePage",
                message.headers(), message.body().encodePrettily());
        JsonArray data = new JsonArray().add(message.body().getString("id"));

        dbClient.updateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), data, res -> {
            if (res.succeeded()) {
                message.reply("ok");
            } else {
                reportQueryError(message, res.cause());
            }
        });
    }

    private void reportQueryError(Message<JsonObject> message, Throwable cause) {
        LOGGER.error("Database query error", cause);
        message.fail(ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
    }
}
