package hu.blackbelt.judo.operation.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    public Collection<AbstractGeneratedScript.Container> filter(Collection<AbstractGeneratedScript.Container> containers, Predicate<AbstractGeneratedScript.Holder<AbstractGeneratedScript.Container>> predicate) {
        return containers.stream().filter(container -> {
            return predicate.test(containerHolder(container));
        }).collect(Collectors.toSet());
    }

    public Boolean exists(Collection<AbstractGeneratedScript.Container> containers, Predicate<AbstractGeneratedScript.Holder<AbstractGeneratedScript.Container>> predicate) {
        return !filter(containers, predicate).isEmpty();
    }

    public Boolean forAll(Collection<AbstractGeneratedScript.Container> containers, Predicate<AbstractGeneratedScript.Holder<AbstractGeneratedScript.Container>> predicate) {
        return filter(containers, predicate).size() == containers.size();
    }

    public <T extends Comparable<T>> T max(Collection<AbstractGeneratedScript.Container> containers, Function<AbstractGeneratedScript.Holder<AbstractGeneratedScript.Container>, T> generator) {
        return containers.stream().map(container -> {
            return generator.apply(containerHolder(container));
        }).max(Comparator.naturalOrder()).get();
    }

    public <T extends Comparable<T>> T min(Collection<AbstractGeneratedScript.Container> containers, Function<AbstractGeneratedScript.Holder<AbstractGeneratedScript.Container>, T> generator) {
        return containers.stream().map(container -> {
            return generator.apply(containerHolder(container));
        }).min(Comparator.naturalOrder()).get();
    }

    public BigInteger sumInteger(Collection<AbstractGeneratedScript.Container> containers, Function<AbstractGeneratedScript.Holder<AbstractGeneratedScript.Container>, BigInteger> generator) {
        return containers.stream().map(container -> {
            return generator.apply(containerHolder(container));
        }).reduce(BigInteger.ZERO, BigInteger::add);
    }

    public BigDecimal sumDecimal(Collection<AbstractGeneratedScript.Container> containers, Function<AbstractGeneratedScript.Holder<AbstractGeneratedScript.Container>, BigDecimal> generator) {
        return containers.stream().map(container -> {
            return generator.apply(containerHolder(container));
        }).reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    public BigDecimal avg(Collection<AbstractGeneratedScript.Container> containers, Function<AbstractGeneratedScript.Holder<AbstractGeneratedScript.Container>, BigDecimal> generator) {
        int count = containers.size();
        BigDecimal sum = (BigDecimal) containers.stream().map(container -> {
            return generator.apply(containerHolder(container));
        }).reduce(BigDecimal.ZERO, BigDecimal::add);
        if (count > 0) {
            return sum.divide(BigDecimal.valueOf(count));
        } else {
            return BigDecimal.ZERO;
        }
    }

    private static AbstractGeneratedScript.Holder<AbstractGeneratedScript.Container> containerHolder(AbstractGeneratedScript.Container container) {
        return new AbstractGeneratedScript.Holder<>(container);
    }

}
