package com.github.floverde.azure.datalake.nio.matchers;

import com.hrakaroo.glob.GlobPattern;
import com.hrakaroo.glob.MatchingEngine;
import java.util.regex.PatternSyntaxException;
import java.nio.file.PathMatcher;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Path matcher that uses glob syntax for pattern matching.
 */
public final class GlobPathMatcher implements PathMatcher
{
    /**
     * The compiled glob pattern used for matching paths.
     */
    private final MatchingEngine pattern;

    /**
     * Creates a new path matcher for the given glob pattern.
     *
     * @param pattern the glob pattern to compile; must not be {@code null}.
     * @throws NullPointerException if {@code pattern} is {@code null}.
     * @throws PatternSyntaxException if the glob pattern is invalid.
     */
    public GlobPathMatcher(final String pattern) throws PatternSyntaxException {
        Objects.requireNonNull(pattern, "Glob Pattern can not be null");
        try {
            this.pattern = GlobPattern.compile(pattern);
        } catch (final RuntimeException ex) {
            throw new PatternSyntaxException(ex.getMessage(), pattern, -1);
        }
    }

    /**
     * Tells if given path matches this matcher's pattern.
     *
     * @param path the path to match (must not be {@code null}).
     * @return {@code true} if the path matches.
     */
    @Override
    public boolean matches(final Path path) {
        return this.pattern.matches(path.toString());
    }
}
