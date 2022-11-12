package com.scibrazeau.dbunitreporter;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.bigquery.*;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import one.util.streamex.StreamEx;
import org.apache.commons.io.output.TeeOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

import java.io.*;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.regex.Pattern;

@SuppressWarnings("java:S3008")
public class BQTestInterceptor implements Extension, InvocationInterceptor {

    private static final String SHORT_SHA = getFromEnvOrGit("SHORT_SHA", "git rev-parse --short HEAD");
    private static final String BRANCH_NAME = getFromEnvOrGit("BRANCH_NAME", "git rev-parse --abbrev-ref HEAD");
    private static final String BRANCH_TAG = getBranchTag();

    private static final String DB_NAME = getPropValue("DBTI_DB_NAME", "testresults");
    private static final String TABLE_NAME = getPropValue("DBTI_TABLE_NAME", "testresults");

    private static String COMPUTER_NAME = getComputerName();
    private final BigQuery bigQuery;
    private final Table table;


    public BQTestInterceptor() {
        var builder = BigQueryOptions.newBuilder();
        if (!StringUtils.isEmpty(getPropValue("PROJECT_ID"))) {
            builder.setProjectId(getPropValue("PROJECT_ID"));
        }
        try {
            GoogleCredentials base = GoogleCredentials.getApplicationDefault();
            var toImpersonate = getPropValue("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT");
            if (!StringUtils.isEmpty(toImpersonate)) {
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
        } catch (IOException e) {
            throw (Error) ExceptionUtils.rethrow(e);
        }
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
            logResult(invocationContext, extensionContext, startTime, success, os.toString(Charset.defaultCharset()));
        }
    }

    private void logResult(ReflectiveInvocationContext<Method> invocationContext, ExtensionContext extensionContext, LocalDateTime startTime, boolean success, String stdout) {
        LocalDateTime endTime = LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS);
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
        payload.put("module_name", getModuleNameInternal());
        payload.put("package_name", method.getDeclaringClass().getPackageName());
        payload.put("class_name", method.getDeclaringClass().getSimpleName());
        payload.put("method_name", methodName);
        payload.put("method_desc", methodDesc);
        payload.put("start_time", startTime.format(DateTimeFormatter.ISO_DATE_TIME));
        payload.put("end_time", endTime.format(DateTimeFormatter.ISO_DATE_TIME));
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
        var rowToInsert = InsertAllRequest.newBuilder(table.getTableId()).addRow(payload).build();
        var response = this.bigQuery.insertAll(rowToInsert);
        if (response.hasErrors()) {
            Assertions.fail("Failed to log results to BigQuery: " +
                    StreamEx.of(response.getInsertErrors().values()).joining("\n")
            );
        }
    }

    private String getModuleNameInternal() {
        return new File(".").getAbsoluteFile().getParentFile().getName();
    }

    private static String getBranchTag() {
        var branchName = BRANCH_NAME.toLowerCase();
        var matcher = Pattern.compile("\b([a-z]+-\\d+)\b").matcher(branchName);
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
        var sysProp = System.getProperty(env);
        if (!Strings.isNullOrEmpty(sysProp)) {
            return sysProp;
        }
        sysProp = System.getenv(env);
        if (!Strings.isNullOrEmpty(sysProp)) {
            return sysProp;
        }
        return defaultValue;
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
