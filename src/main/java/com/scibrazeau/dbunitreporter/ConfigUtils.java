package com.scibrazeau.dbunitreporter;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 *  Internal class used to read coniguration for DB Unit Repoter
 */

/* package */ class ConfigUtils {
    public static final String KN_JDBC_URL = "DBUR_JDBC_URL";
    public static final String KN_USER = "DBUR_USER";
    public static final String KN_PWD = "DBUR_PWD";
    public static final String KN_HOST = "DBUR_HOST";
    public static final String KN_PORT = "DBUR_PORT";
    public static final String KN_TABLE_NAME = "DBUR_TABLE_NAME";
    public static final String KN_DB_NAME = "DBUR_DB_NAME";
    public static final String KN_BRANCH_NAME = "DBUR_BRANCH_NAME";
    public static final String KN_SHORT_SHA = "DBUR_SHORT_SHA";


    public static String getProperty(String propName) {
        return getProperty(propName, null, false);
    }
    public static String getProperty(String propName, String defaultValue, boolean tryWithoutDBUR) {
        if (System.getProperties().contains(propName)) {
            return System.getProperty(propName);
        }
        if (System.getenv().containsKey(propName)) {
            return System.getenv(propName);
        }
        if (tryWithoutDBUR && propName.startsWith("DBUR_")) {
            return getProperty(propName.substring(5), defaultValue, false);
        }
        return defaultValue;
    }

    public static String getComputerName()
    {
        var cn = ConfigUtils.getProperty("COMPUTERNAME");
        if (!StringUtils.isEmpty(cn)) {
            return cn;
        }
        cn = ConfigUtils.getProperty("HOSTNAME");
        if (!StringUtils.isEmpty(cn)) {
            return cn;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "$COMPUTERNAME unset";
        }
    }


    public static String getFromPropOrGitCmd(String propertyName, String gitCmd) {
        var propVal = getProperty(propertyName, null, true);
        if (propVal != null) {
            return propVal;
        }
        try {
            //noinspection deprecation
            Process process = Runtime.getRuntime().exec(gitCmd);
            process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            return reader.readLine();
        } catch (IOException | InterruptedException e) {
            return "$" + propertyName + " unset";
        }
    }


}
