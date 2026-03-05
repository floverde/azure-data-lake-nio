package com.github.floverde.azure.datalake.nio.matchers;

import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.util.regex.PatternSyntaxException;
import java.util.regex.Pattern;
import java.util.Objects;

/**
 * Path matcher that uses regular expression syntax for pattern matching.
 */
public final class RegexPathMatcher implements PathMatcher
{
    /**
     * The compiled regular expression pattern used for matching paths.
     */
    private final Pattern pattern;

    /**
     * Creates a new path matcher for the given regular expression pattern.
     *
     * @param pattern the regular expression pattern to compile; must not be {@code null}.
     * @throws PatternSyntaxException if the regular expression pattern is invalid.
     * @throws NullPointerException if {@code pattern} is {@code null}.
     */
    public RegexPathMatcher(final String pattern) throws PatternSyntaxException {
        this.pattern = Pattern.compile(Objects.requireNonNull(pattern,
                "Regex Pattern can not be null"));
    }

    /**
     * Tells if given path matches this matcher's pattern.
     *
     * @param path the path to match (must not be {@code null}).
     * @return {@code true} if the path matches.
     */
    @Override
    public boolean matches(final Path path) {
        return this.pattern.matcher(path.toString()).matches();
    }
}
