package io.vertx.guides.wiki.database;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.serviceproxy.ServiceBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * As most of the database handling code has been moved to WikiDatabaseServiceImpl, the
 * WikiDatabaseVerticle class now consists of 2 methods: the start method to register the service and a
 * utility method to load SQL queries:
 */
public class WikiDatabaseVerticle extends AbstractVerticle {
    public static final String CONFIG_WIKIDB_JDBC_URL = "wikidb.jdbc.url";
    public static final String CONFIG_WIKIDB_JDBC_DRIVER_CLASS = "wikidb.jdbc.driver_class";
    public static final String CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE = "wikidb.jdbc.max_pool_size";
    public static final String CONFIG_WIKIDB_SQL_QUERIES_RESOURCE_FILE = "wikidb.sqlqueries.resource.file";
    public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

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
        HashMap<SqlQuery, String> sqlQueries = loadSqlQueries();

        JDBCClient dbClient = JDBCClient.createShared(vertx, new JsonObject()
                .put("url", config().getString(CONFIG_WIKIDB_JDBC_URL, "jdbc:hsqldb:file:db/wiki"))
                .put("driver_class", config().getString(CONFIG_WIKIDB_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver"))
                .put("max_pool_size", config().getInteger(CONFIG_WIKIDB_JDBC_MAX_POOL_SIZE, 30)));

        WikiDatabaseService.create(dbClient, sqlQueries, ready -> {
            if (ready.succeeded()) {
                /**
                 * We register the service here.
                 * Registering a service requires an interface class, a Vert.x context, an implementation and an event
                 * bus destination.
                 */
                ServiceBinder binder = new ServiceBinder(vertx);
                binder.setAddress(CONFIG_WIKIDB_QUEUE)
                        .register(WikiDatabaseService.class, ready.result());
                startFuture.complete();
            } else {
                startFuture.fail(ready.cause());
            }
        });

    }


    private HashMap<SqlQuery, String> loadSqlQueries() throws IOException {
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

        HashMap<SqlQuery, String> sqlQueries = new HashMap<>();
        // We use the SqlQuery enumeration type to avoid string constants later in the code.
        sqlQueries.put(SqlQuery.CREATE_PAGES_TABLE, queriesProps.getProperty("create-pages-table"));
        sqlQueries.put(SqlQuery.ALL_PAGES, queriesProps.getProperty("all-pages"));
        sqlQueries.put(SqlQuery.GET_PAGE, queriesProps.getProperty("get-page"));
        sqlQueries.put(SqlQuery.CREATE_PAGE, queriesProps.getProperty("create-page"));
        sqlQueries.put(SqlQuery.SAVE_PAGE, queriesProps.getProperty("save-page"));
        sqlQueries.put(SqlQuery.DELETE_PAGE, queriesProps.getProperty("delete-page"));
        sqlQueries.put(SqlQuery.ALL_PAGES_DATA, queriesProps.getProperty("all-pages-data"));
        return sqlQueries;
    }

}
