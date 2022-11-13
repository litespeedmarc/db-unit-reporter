package com.scibrazeau.dbunitreporter;

import one.util.streamex.StreamEx;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class TagUtils {
    private static final ThreadLocal<Set<String>> EXTRA_TAGS = new ThreadLocal<>();

    private TagUtils() {
    }

    /**
     * Add a TAG to tests that are run in the current thread.  Ideally, tests should
     * be using the @Tag Junit annotation.  That doesn't always work, or is more work
     * than you'd like.  Using this method will automatically add the specific tag.
     * @param   tags   The tag(s) to add
     */
    public static void addTag(String ... tags) {
        var set = EXTRA_TAGS.get();
        if (set != null) {
            set.addAll(
                    StreamEx.of(tags)
                            .flatMap(tag -> Arrays.stream(tag.split(","))).toList()
            );
        }

    }


    /* package */ static Collection<String> getExtraTags() {
        return EXTRA_TAGS.get();
    }

    public static void init() {
        EXTRA_TAGS.set(new HashSet<>());
    }

    public static void remove() {
        EXTRA_TAGS.remove();
    }
}
