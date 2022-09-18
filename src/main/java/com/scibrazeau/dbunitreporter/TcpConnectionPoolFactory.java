package com.scibrazeau.dbunitreporter;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;

import static com.scibrazeau.dbunitreporter.ConfigUtils.*;

/**
 * Internal class, used to obtain a connection to the DB
 * to which results should be logged to.
 * Set one or more of the environment variables
 * below for this to work.
 */
/* package */ class TcpConnectionPoolFactory {

  public static DataSource createConnectionPool() {
    // The configuration object specifies behaviors for the connection pool.
    HikariConfig config = new HikariConfig();

    // The following URL is equivalent to setting the config options below:
    // jdbc:postgresql://<INSTANCE_HOST>:<DB_PORT>/<DB_NAME>?user=<DB_USER>&password=<DB_PASS>
    // See the link below for more info on building a JDBC URL for the Cloud SQL JDBC Socket Factory
    // https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory#creating-the-jdbc-url

    // Configure which instance and what database user to connect with.
    config.setJdbcUrl(getJdbcUrl());
    var user = getProperty(KN_USER);
    if (user != null) {
      config.setUsername(user); // e.g. "root", "postgres"
    }
    var pwd = getProperty(KN_PWD);
    config.setPassword(pwd);

    // Initialize the connection pool using the configuration object.
    return new HikariDataSource(config);
  }

  private static String getJdbcUrl() {
    var url = getProperty(KN_JDBC_URL);
    if (!StringUtils.isEmpty(url)) {
      return url;
    }
    String port = getProperty(KN_PORT, "5432", false);
    String host = getProperty(KN_HOST, "localhost", false);
    String dbName = getProperty(KN_DB_NAME, "test-db", false);

    return String.format("jdbc:postgresql://%s:%s/%s", host, port, dbName);
  }

}