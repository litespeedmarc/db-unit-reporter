package com.scibrazeau.dbunitreporter;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import one.util.streamex.StreamEx;
import org.apache.commons.io.output.TeeOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.*;

import java.io.*;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.extension.ExtensionContext.Namespace.GLOBAL;

@SuppressWarnings("java:S3008")
public class BQTestInterceptor implements Extension, BeforeAllCallback, InvocationInterceptor, ExtensionContext.Store.CloseableResource {

    private static final String SHORT_SHA = getFromEnvOrGit("SHORT_SHA", "git rev-parse --short HEAD");
    private static final String BRANCH_NAME = getFromEnvOrGit("BRANCH_NAME", "git rev-parse --abbrev-ref HEAD");
    private static final String BRANCH_TAG = getBranchTag(BRANCH_NAME);


    private static final String DB_NAME = getPropValue("DB_NAME", "testresults");
    private static final String TABLE_NAME = getPropValue("TABLE_NAME", "testresults");

    private static final String COMPUTER_NAME = getComputerName();
    private static final String MODULE_NAME = getModuleNameInternal();
    private static final Logger LOGGER = LogManager.getLogger(BQTestInterceptor.class);
    private static final int INSERT_QUEUE_SIZE = 1000;

    private final ArrayBlockingQueue<Map<String, Object>> messages = new ArrayBlockingQueue<>(INSERT_QUEUE_SIZE);
    private BigQuery bigQuery;
    private Table table;
    private Thread logInserter;
    private boolean started;


    public void lazyLoad() {
        if (this.table != null) {
            return;
        }
        var projectId = getPropValue("PROJECT_ID");
        var toImpersonate = getPropValue("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT");
        LOGGER.info("Initiating logging of test results to BigQuery\n   {}\n   {}\n   {}",
                String.format("%-40.40s%-40.40s%s",
                        "short_sha=" + SHORT_SHA,
                        "branch_tag=" + BRANCH_TAG,
                        "branch_name=" + BRANCH_NAME
                ),
                String.format("%-40.40s%-40.40s%s",
                        "computer_name=" + COMPUTER_NAME,
                        "db_name=" + DB_NAME,
                        "table_name=" + TABLE_NAME
                ),
                String.format("%-40.40s%-40.40s%s",
                        "module_name=" + MODULE_NAME,
                        "project_id=" + projectId,
                        "impersonate=" + toImpersonate
                )
        );
        var builder = BigQueryOptions.newBuilder();
        if (!StringUtils.isEmpty(projectId)) {
            builder.setProjectId(projectId);
        }
        try {
            GoogleCredentials base = GoogleCredentials.getApplicationDefault();
            if (!StringUtils.isEmpty(toImpersonate)) {
                LOGGER.debug("google_impersonate_service_account={}", toImpersonate);
                base = ImpersonatedCredentials.create(
                        base,
                        toImpersonate,
                        Collections.emptyList(),
                        ImmutableList.<String>builder()
                                .add("https://www.googleapis.com/auth/cloud-platform")
                                .add("https://www.googleapis.com/auth/datastore")
                                .build(),
                        0
                );
            }
            builder.setCredentials(base);
            this.bigQuery = builder.build().getService();
            this.table = createTableIfMissing();

            LOGGER.trace("Started LogInserter Thread");
            this.logInserter = new Thread(this::continuouslyInsertLogs, "logInserter");
            this.logInserter.start();
        } catch (IOException e) {
            throw (Error) ExceptionUtils.rethrow(e);
        }
    }

    private void continuouslyInsertLogs() {
        Stopwatch sw = Stopwatch.createStarted();
        var insertBuilder = InsertAllRequest.newBuilder(table.getTableId());
        long numberOfTests = 0;
        int numRows = 0;
        while (true) {
            Map<String, Object> next = null;
            try {
                next = this.messages.poll(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                ExceptionUtils.rethrow(e);
            }
            boolean finished = next != null && next.isEmpty();
            if (next != null && !next.isEmpty()) {
                insertBuilder.addRow(next);
                numRows++;
                numberOfTests++;
            }
            if (sw.elapsed(TimeUnit.MILLISECONDS) > 500 || finished || this.messages.size() >= INSERT_QUEUE_SIZE) {
                if (numRows > 0) {
                    var rowToInsert = insertBuilder.build();
                    var response = this.bigQuery.insertAll(rowToInsert);
                    if (response.hasErrors()) {
                        LOGGER.warn("Failed to insert some test results into bigquery table {}.{}. See errors below.", DB_NAME, TABLE_NAME);
                        StreamEx.of(response.getInsertErrors().values())
                                .flatMap(Collection::stream)
                                .map(BigQueryError::getMessage)
                                .forEach(LOGGER::warn);
                    }
                    insertBuilder = InsertAllRequest.newBuilder(table.getTableId());
                    numRows = 0;
                    sw.reset();
                }
                if (finished) {
                    break;
                }
            }
        }
        LOGGER.info("Logging to big query complete. {} tests reported", numberOfTests);
    }


    private Table createTableIfMissing() {
        var schema =
                Schema.of(
                        Field.of("branch_name", StandardSQLTypeName.STRING),
                        Field.of("branch_tag", StandardSQLTypeName.STRING),
                        Field.of("short_sha", StandardSQLTypeName.STRING),
                        Field.of("computer_name", StandardSQLTypeName.STRING),
                        Field.of("module_name", StandardSQLTypeName.STRING),
                        Field.of("package_name", StandardSQLTypeName.STRING),
                        Field.of("class_name", StandardSQLTypeName.STRING),
                        Field.of("method_name", StandardSQLTypeName.STRING),
                        Field.of("method_desc", StandardSQLTypeName.STRING),
                        Field.of("start_time", StandardSQLTypeName.DATETIME),
                        Field.of("end_time", StandardSQLTypeName.DATETIME),
                        Field.of("duration", StandardSQLTypeName.INT64),
                        Field.of("stdout", StandardSQLTypeName.STRING),
                        Field.of("success", StandardSQLTypeName.BOOL),
                        Field.newBuilder("tags", StandardSQLTypeName.STRING)
                                .setMode(Field.Mode.REPEATED)
                                .build()
                );
        var tableId = TableId.of(DB_NAME, TABLE_NAME);
        var tableDefinition = StandardTableDefinition.of(schema);
        var existingTable = bigQuery.getTable(DB_NAME, TABLE_NAME);
        if (existingTable == null || !existingTable.exists()) {
            var tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
            return bigQuery.create(tableInfo);
        } else {
            existingTable.toBuilder().setDefinition(tableDefinition).build();
            return existingTable.update();
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

    @Override
    public void close() throws Throwable {
        if (this.logInserter == null) {
            // we get here when IS_CI != "true", which would mean that
            // we're not logging anything.
            return;
        }
        this.messages.put(new HashMap<>());
        this.logInserter.join();
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!started) {
            started = true;
            // Your "before all tests" startup logic goes here
            // The following line registers a callback hook when the root test context is shut down
            context.getRoot().getStore(GLOBAL).put("wait for BQ logging to complete", this);
        }

    }

    private interface Wrapped {
        void accept(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext) throws Throwable;
    }

    public void wrap(Invocation<Void> invocation, ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext, Wrapped wrapped) throws Throwable {
        TagUtils.init();
        if (!"true".equals(getPropValue("IS_CI"))) {
            wrapped.accept(invocation, invocationContext, extensionContext);
            return;
        }
        if (this.table != null) {
            synchronized (this) {
                lazyLoad();
            }
        }
        var originalOut = System.out;
        var originalErr = System.out;
        var os = new org.apache.commons.io.output.ByteArrayOutputStream();
        var newOut = new PrintStream(new TeeOutputStream(originalOut, os));
        var newErr = new PrintStream(new TeeOutputStream(originalErr, os));
        var startTime = Instant.now();
        boolean success = false;
        try {
            System.setOut(newOut);
            System.setErr(newErr);
            wrapped.accept(invocation, invocationContext, extensionContext);
            success = true;
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
            os.close();
            logResult(invocationContext, extensionContext, startTime, success, os.toString(Charset.defaultCharset()));
            TagUtils.remove();
        }
    }

    private void logResult(ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext, Instant startTime, boolean success, String stdout) throws InterruptedException {
        var endTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        startTime = startTime.truncatedTo(ChronoUnit.MILLIS);
        var method = invocationContext.getExecutable();
        String methodName = method.getName();
        String methodDesc = "(" + (invocationContext.getArguments() == null || invocationContext.getArguments().isEmpty() ?
                "" :
                StreamEx.of(invocationContext.getArguments())
                        .joining(",")) + ")";
        var payload = new HashMap<String, Object>();
        payload.put("branch_name", BRANCH_NAME);
        payload.put("branch_tag", BRANCH_TAG);
        payload.put("short_sha", SHORT_SHA);
        payload.put("computer_name", COMPUTER_NAME);
        payload.put("module_name", MODULE_NAME);
        payload.put("package_name", method.getDeclaringClass().getPackageName());
        payload.put("class_name", method.getDeclaringClass().getSimpleName());
        payload.put("method_name", methodName);
        payload.put("method_desc", methodDesc);
        payload.put("start_time", StringUtils.chop(startTime.toString()));
        payload.put("end_time", StringUtils.chop(endTime.toString()));
        payload.put("duration", ChronoUnit.MILLIS.between(startTime, endTime));
        payload.put("stdout", stdout);
        payload.put("success", success);
        var uniqueTags = StreamEx.of(extensionContext.getTags())
                .append(TagUtils.getExtraTags())
                .flatMap(s -> Arrays.stream(s.split(",")))
                .map(StringUtils::normalizeSpace)
                .map(StringUtils::toRootUpperCase)
                .filter(StringUtils::isNotEmpty)
                .sorted()
                .distinct()
                .toArray(new String[]{});
        payload.put("tags", uniqueTags);
        this.messages.put(payload);

    }

    private static String getModuleNameInternal() {
        return new File(".").getAbsoluteFile().getParentFile().getName();
    }

    /* package */ static String getBranchTag(String name) {
        var branchName = name.toLowerCase();
        var matcher = Pattern.compile("([a-z]+-\\d+)").matcher(branchName);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return branchName;
        }
    }

    private static String getFromEnvOrGit(String env, String gitCmd) {
        var propValue = getPropValue(env);
        if (!Strings.isNullOrEmpty(propValue)) {
            return propValue;
        }
        try {
            //noinspection deprecation
            Process process = Runtime.getRuntime().exec(gitCmd);
            process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            return reader.readLine();
        } catch (IOException | InterruptedException e) {
            return ExceptionUtils.rethrow(e);
        }
    }

    private static String getPropValue(String env) {
        return getPropValue(env, null);
    }
    private static String getPropValue(String env, String defaultValue) {
        return StreamEx.of(
                System.getProperty("dbunitreporter_" + env),
                System.getenv("dbunitreporter_" + env),
                System.getProperty(env),
                System.getenv(env)
        ).filter(StringUtils::isNotEmpty)
         .findFirst()
         .orElse(defaultValue);
    }

    private static String getComputerName()
    {
        var compName = getPropValue("COMPUTERNAME");
        if (!Strings.isNullOrEmpty(compName)) {
            return compName;
        }
        compName = getPropValue("HOSTNAME");
        if (!Strings.isNullOrEmpty(compName)) {
            return compName;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "Unknown";
        }
    }

}
