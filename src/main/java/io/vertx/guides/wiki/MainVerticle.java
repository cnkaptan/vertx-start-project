package io.vertx.guides.wiki;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;

public class MainVerticle extends AbstractVerticle {

    /**
     * The database operations will be typical create, read, update, delete operations. To get us started, we
     * simply store the corresponding SQL queries as static fields of the MainVerticle class. Note that they
     * are written in a SQL dialect that HSQLDB understands, but that other relational databases may not
     * necessarily support:
     */
    private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists Pages (Id integer identity primary key,Name varchar(255) unique, Content clob)";
    private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
    private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
    private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
    private static final String SQL_ALL_PAGES = "select Name from Pages";
    private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";

    private JDBCClient dbClient;
    private Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);


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
                    if (create.failed()){
                        future.fail(create.cause());
                    }else{
                        LOGGER.error("Database preparation error", create.cause());
                        future.complete();
                    }
                });
            }
        });
        return future;
    }

    private Future<Void> startHttpServer() {
        Future<Void> future = Future.future();

        return future;
    }
}
