package com.scibrazeau.dbunitreporter;

import one.util.streamex.StreamEx;
import org.apache.commons.io.output.TeeOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import javax.sql.DataSource;
import java.io.*;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;

/**
 * This is the main interceptor that'll log to the database.
 * It should be automatically loaded via the
 * junit.jupiter.extensions.autodetection.enabled=true
 * property set in src/main/resources/junit-platform.properties
 * If you also include this file in your project, then you will
 * nned to make sure to set the above property to true.
 *
 */
public class TestDbLoggerInterceptor implements Extension, InvocationInterceptor {
    private static final String SHORT_SHA = ConfigUtils.getFromPropOrGitCmd(ConfigUtils.KN_SHORT_SHA, "git rev-parse --short HEAD");
    private static final String BRANCH_NAME = ConfigUtils.getFromPropOrGitCmd(ConfigUtils.KN_BRANCH_NAME, "git rev-parse --abbrev-ref HEAD");
    private static final String COMPUTER_NAME = ConfigUtils.getComputerName();
    private static final DataSource DS = TcpConnectionPoolFactory.createConnectionPool();
    private static final String RESULTS_TABLE = ConfigUtils.getProperty(ConfigUtils.KN_TABLE_NAME, "test_results", false);

    private static final String createTableSQL = """
            CREATE TABLE IF NOT EXISTS %s (
                    id              UUID             NOT NULL PRIMARY KEY,
                    git_branch      VARCHAR(255)     NOT NULL,
                    git_hash        VARCHAR(32)      NOT NULL,
                    host_name       VARCHAR(255)     NOT NULL,
                    module_name     VARCHAR(100)     NOT NULL,
                    package_name    VARCHAR(255)     NOT NULL,
                    class_name      VARCHAR(255)     NOT NULL,
                    test_name       VARCHAR(255)     NOT NULL,
                    test_desc       VARCHAR(1000)    NOT NULL,
                    tags            VARCHAR(1000)    NOT NULL,
                    start_time      TIMESTAMPTZ      NOT NULL,
                    end_time        TIMESTAMPTZ      NOT NULL,
                    success         bool             NOT NULL,
                    error           bool             NOT NULL,
                    console         text             NOT NULL
            )
        """.formatted(RESULTS_TABLE);

    private static final String startTestSql = "INSERT INTO %s VALUES (%s)".formatted(
            RESULTS_TABLE, (StringUtils.repeat("?, ", 14) + " ?")
    );

    static {
        synchSchema();
    }


    private static void synchSchema() {
        try (
                Connection con = DS.getConnection();
                PreparedStatement ps = con.prepareStatement(createTableSQL)
        ) {
            ps.executeUpdate();
        } catch (SQLException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public void interceptTestTemplateMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) throws Throwable {
        wrap(invocation, invocationContext, extensionContext, InvocationInterceptor.super::interceptTestTemplateMethod);
    }

    @Override
    public void interceptTestMethod(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) throws Throwable {
        wrap(invocation, invocationContext, extensionContext, InvocationInterceptor.super::interceptTestMethod);
    }

    private interface Wrapped {
        void accept(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) throws Throwable;
    }

    public void wrap(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext, Wrapped wrapped) throws Throwable {
        var originalOut = System.out;
        var originalErr = System.out;
        var os = new org.apache.commons.io.output.ByteArrayOutputStream();
        var newOut = new PrintStream(new TeeOutputStream(originalOut, os));
        var newErr = new PrintStream(new TeeOutputStream(originalErr, os));
        LocalDateTime startTime = LocalDateTime.now();
        boolean success = false;
        try {
            System.setOut(newOut);
            System.setErr(newErr);
            TagUtils.init();
            wrapped.accept(invocation, invocationContext, extensionContext);
            success = true;
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
            os.close();
            logResult(invocationContext, extensionContext.getTags(), startTime, success, os.toString(Charset.defaultCharset()));
        }
    }


    private void logResult(ReflectiveInvocationContext<Method> invocationContext, Set<String> tags, LocalDateTime startTime, boolean success, String stdout) throws SQLException {
        LocalDateTime endTime = LocalDateTime.now();
        var method = invocationContext.getExecutable();
        UUID id = UUID.randomUUID();
        String methodName = method.getName();
        String methodDesc = "(" + (invocationContext.getArguments() == null || invocationContext.getArguments().isEmpty() ?
                "" :
                StreamEx.of(invocationContext.getArguments())
                        .joining(",")) + ")";
        int i = 0;
        try (
                Connection con = DS.getConnection();
                PreparedStatement ps = con.prepareStatement(startTestSql)
        ) {
            ps.setObject(++i, id, Types.OTHER);
            ps.setString(++i, BRANCH_NAME);
            ps.setString(++i, SHORT_SHA);
            ps.setString(++i, COMPUTER_NAME);
            ps.setString(++i, getModuleNameInternal());
            ps.setString(++i, method.getDeclaringClass().getPackageName());
            ps.setString(++i, method.getDeclaringClass().getSimpleName());
            ps.setString(++i, methodName);
            ps.setString(++i, methodDesc);
            ps.setString(++i,
                    StreamEx.of(tags)
                            .append(TagUtils.getExtraTags())
                            .flatMap(s -> Arrays.stream(s.split(",")))
                            .map(StringUtils::normalizeSpace)
                            .map(StringUtils::toRootUpperCase)
                            .filter(StringUtils::isNotEmpty)
                            .sorted()
                            .distinct()
                            .joining(",")
            );
            ps.setObject(++i, startTime);
            ps.setObject(++i, endTime);
            ps.setBoolean(++i, success);
            ps.setBoolean(++i, !success);
            ps.setString(++i, stdout);
            ps.executeUpdate();
        }
    }


    private String getModuleNameInternal() {
        return new File(".").getAbsoluteFile().getParentFile().getName();
    }
}
