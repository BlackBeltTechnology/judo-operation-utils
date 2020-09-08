package hu.blackbelt.judo.operation.utils;

import java.math.BigInteger;
import java.util.Collection;
import java.util.regex.Pattern;

public class FunctionRunner {
    private final AbstractGeneratedScript script;

    public FunctionRunner(AbstractGeneratedScript script) {
        this.script = script;
    }

    public AbstractGeneratedScript.Container any(Collection<AbstractGeneratedScript.Container> collection) {
        return head(collection);
    }

    public AbstractGeneratedScript.Container head(Collection<AbstractGeneratedScript.Container> collection) {
        if (collection == null || collection.isEmpty()) {
            return null;
        } else {
            return collection.iterator().next();
        }
    }

    public BigInteger count(Collection<AbstractGeneratedScript.Container> collection) {
        if (collection == null || collection.isEmpty()) {
            return BigInteger.ZERO;
        } else {
            return BigInteger.valueOf(collection.size());
        }
    }

    public Boolean empty(Collection<AbstractGeneratedScript.Container> collection) {
        return collection == null || collection.isEmpty();
    }

    public String substring(String s, BigInteger position, BigInteger length) {
        if (s == null) {
            return null;
        }
        int start = position.intValue() - 1;
        int end = start + length.intValue();
        String result;
        if (end < s.length()) {
            result = s.substring(start, end);
        } else {
            result = s.substring(start);
        }
        return result;
    }

    public Boolean matches(String text, String pattern) {
        if (text == null) {
            return null;
        }
        return text.matches(pattern);
    }


    public String replace(String text, String pattern, String replacement) {
        if (text == null) {
            return null;
        }
        return text.replaceAll(Pattern.quote(pattern), replacement);
    }


}
