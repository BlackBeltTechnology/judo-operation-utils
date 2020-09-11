package hu.blackbelt.judo.operation.utils;

import hu.blackbelt.judo.operation.utils.AbstractGeneratedScript.Container;
import hu.blackbelt.judo.operation.utils.AbstractGeneratedScript.Holder;
import hu.blackbelt.judo.operation.utils.AbstractGeneratedScript.SortOrderBy;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FunctionRunner {
    private final AbstractGeneratedScript script;

    public FunctionRunner(AbstractGeneratedScript script) {
        this.script = script;
    }

    public Container any(Collection<Container> collection) {
        return head(collection);
    }

    public Container head(Collection<Container> collection) {
        if (collection == null || collection.isEmpty()) {
            return null;
        } else {
            return collection.iterator().next();
        }
    }

    public BigInteger count(Collection<Container> collection) {
        if (collection == null || collection.isEmpty()) {
            return BigInteger.ZERO;
        } else {
            return BigInteger.valueOf(collection.size());
        }
    }

    public Boolean empty(Collection<Container> collection) {
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

    public Collection<Container> filter(Collection<Container> containers, Predicate<Holder<Container>> predicate) {
        return containers.stream().filter(container -> {
            return predicate.test(containerHolder(container));
        }).collect(Collectors.toSet());
    }

    public Boolean exists(Collection<Container> containers, Predicate<Holder<Container>> predicate) {
        return !filter(containers, predicate).isEmpty();
    }

    public Boolean forAll(Collection<Container> containers, Predicate<Holder<Container>> predicate) {
        return filter(containers, predicate).size() == containers.size();
    }

    public <T extends Comparable<T>> T max(Collection<Container> containers, Function<Holder<Container>, T> generator) {
        return containers.stream().map(container -> {
            return generator.apply(containerHolder(container));
        }).max(Comparator.naturalOrder()).get();
    }

    public <T extends Comparable<T>> T min(Collection<Container> containers, Function<Holder<Container>, T> generator) {
        return containers.stream().map(container -> {
            return generator.apply(containerHolder(container));
        }).min(Comparator.naturalOrder()).get();
    }

    public BigInteger sumInteger(Collection<Container> containers, Function<Holder<Container>, BigInteger> generator) {
        return containers.stream().map(container -> {
            return generator.apply(containerHolder(container));
        }).reduce(BigInteger.ZERO, BigInteger::add);
    }

    public BigDecimal sumDecimal(Collection<Container> containers, Function<Holder<Container>, BigDecimal> generator) {
        return containers.stream().map(container -> {
            return generator.apply(containerHolder(container));
        }).reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    public BigDecimal avg(Collection<Container> containers, Function<Holder<Container>, BigDecimal> generator) {
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

    public Boolean contains(Collection<Container> containers, Container object) {
        return containers.contains(object);
    }

    public <T extends Comparable> Collection<Container> sort(Collection<Container> containers, SortOrderBy<T>... orderByList) {
        if (orderByList.length == 0) {
            return containers.stream().sorted(Comparator.comparing(Container::getId)).collect(Collectors.toList());
        }
        Collection<Container> sorted = containers;
        for (SortOrderBy<T> orderBy : orderByList) {
            Function<Holder<Container>, T> generatorFunction = orderBy.generator;
            List<Container> sortedContainerList= sorted.stream().map(FunctionRunner::containerHolder).sorted((h1, h2) -> {
                T o1 = generatorFunction.apply(h1);
                T o2 = generatorFunction.apply(h2);
                int compareResult = o1.compareTo(o2);
                if (compareResult == 0) {
                    compareResult = h1.value.getId().compareTo(h2.value.getId());
                }
                return compareResult;
            }).map(Holder::getValue).collect(Collectors.toList());
            if (orderBy.descending) {
                Collections.reverse(sortedContainerList);
            }
            sorted = sortedContainerList;
        }
        return sorted;
    }

    private static Holder<Container> containerHolder(Container container) {
        return new Holder<>(container);
    }

}
