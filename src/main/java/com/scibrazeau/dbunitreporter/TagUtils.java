package com.scibrazeau.dbunitreporter;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class TagUtils {
    private static final ThreadLocal<Set<String>> EXTRA_TAGS = new ThreadLocal<>();

    private TagUtils() {
    }

    /* package */ static void init () {
        EXTRA_TAGS.set(new HashSet<>());
    }

    /**
     * Add a TAG to tests that are run in the current thread.  Ideally, tests should
     * be using the @Tag Junit annotation.  That doesn't always work, or is more work
     * than you'd like.  Using this method will automatically add the specific tag.
     * @param   tag   The tag to add
     */
    public static void addTag(String tag) {
        var set = EXTRA_TAGS.get();
        if (set != null) {
            set.add(tag.replace(',','.'));
        }
    }


    /* package */ static Collection<String> getExtraTags() {
        return EXTRA_TAGS.get();
    }
}
