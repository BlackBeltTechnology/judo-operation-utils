package hu.blackbelt.judo.operation.utils;

import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import hu.blackbelt.judo.operation.utils.AbstractGeneratedScript.Container;
import hu.blackbelt.judo.operation.utils.AbstractGeneratedScript.Holder;
import hu.blackbelt.judo.operation.utils.AbstractGeneratedScript.SortOrderBy;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EOperation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static hu.blackbelt.judo.operation.utils.AbstractGeneratedScript.ENTITY_TYPE;

public class FunctionRunner {
    private final AbstractGeneratedScript script;

    public FunctionRunner(AbstractGeneratedScript script) {
        this.script = script;
    }

    public Container any(Collection<Container> collection) {
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
        if (text == null || pattern == null) {
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

    public <T extends Comparable> List<Container> applySorting(Collection<Holder<Container>> containers, SortOrderBy<T>[] orderByList) {
        Comparator<Holder<Container>> comparator = createComparatorFromSortOrderBys(orderByList);
        Comparator<Holder<Container>> idComparator = Comparator.comparing(h -> h.value.getId());
        comparator = comparator.thenComparing(idComparator);
        return containers.stream().sorted(comparator).map(Holder::getValue).collect(Collectors.toList());
    }

    private <T extends Comparable> Comparator<Holder<Container>> createComparatorFromSortOrderBys(SortOrderBy<T>[] orderByList) {
        int currentIndex = 0;
        Comparator<Holder<Container>> comparator = Comparator.comparing(orderByList[currentIndex].generator);
        if (orderByList[currentIndex].descending) {
            comparator = comparator.reversed();
        }
        currentIndex++;
        while (currentIndex < orderByList.length) {
            Comparator<Holder<Container>> newComparator = Comparator.comparing(orderByList[currentIndex].generator);
            if (orderByList[currentIndex].descending) {
                newComparator = newComparator.reversed();
            }
            comparator = comparator.thenComparing(newComparator);
            currentIndex++;
        }
        return comparator;
    }

    public <T extends Comparable> List<Container> sort(Collection<Container> containers, SortOrderBy<T>... orderByList) {
        if (orderByList.length == 0) {
            return containers.stream().sorted(Comparator.comparing(Container::getId)).collect(Collectors.toList());
        }
        return applySorting(containers.stream().map(FunctionRunner::containerHolder).collect(Collectors.toList()), orderByList);
    }

    public <T extends Comparable> Container head(Collection<Container> collection,  SortOrderBy<T>... orderByList) {
        if (collection == null || collection.isEmpty()) {
            return null;
        } else {
            Collection<Container> result;
            if (orderByList != null) {
                result = sort(collection, orderByList);
            } else {
                result = collection;
            }
            return result.iterator().next();
        }
    }

    public <T extends Comparable> Container tail(Collection<Container> collection,  SortOrderBy<T>... orderByList) {
        if (collection == null || collection.isEmpty()) {
            return null;
        } else {
            List<Container> result;
            if (orderByList != null) {
                result = sort(collection, orderByList);
            } else {
                result = new ArrayList<>(collection);
            }
            Collections.reverse(result);
            return result.iterator().next();
        }
    }

    static <T> Collector<T, ?, List<T>> maxList(Comparator<? super T> comparator) {
        return Collector.of(
                ArrayList::new,
                (list, t) -> {
                    int comparatorResult;
                    if (list.isEmpty() || (comparatorResult = comparator.compare(t, list.get(0))) == 0) {
                        list.add(t);
                    } else if (comparatorResult > 0) {
                        list.clear();
                        list.add(t);
                    }
                },
                (list1, list2) -> {
                    if (list1.isEmpty()) {
                        return list2;
                    }
                    if (list2.isEmpty()) {
                        return list1;
                    }
                    int r = comparator.compare(list1.get(0), list2.get(0));
                    if (r < 0) {
                        return list2;
                    } else if (r > 0) {
                        return list1;
                    } else {
                        list1.addAll(list2);
                        return list1;
                    }
                });
    }

    static <T> Collector<T, ?, List<T>> minList(Comparator<? super T> comparator) {
        return Collector.of(
                ArrayList::new,
                (list, t) -> {
                    int comparatorResult;
                    if (list.isEmpty() || (comparatorResult = comparator.compare(t, list.get(0))) == 0) {
                        list.add(t);
                    } else if (comparatorResult < 0) {
                        list.clear();
                        list.add(t);
                    }
                },
                (list1, list2) -> {
                    if (list1.isEmpty()) {
                        return list2;
                    }
                    if (list2.isEmpty()) {
                        return list1;
                    }
                    int r = comparator.compare(list1.get(0), list2.get(0));
                    if (r < 0) {
                        return list1;
                    } else if (r > 0) {
                        return list2;
                    } else {
                        list1.addAll(list2);
                        return list1;
                    }
                });
    }

    public <T extends Comparable> Collection<Container> heads(Collection<Container> collection,  SortOrderBy<T>... orderByList) {
        if (collection == null || collection.isEmpty()) {
            return null;
        } else {
            return collection.stream().map(FunctionRunner::containerHolder).collect(minList(createComparatorFromSortOrderBys(orderByList))).stream().map(Holder::getValue).collect(Collectors.toList());
        }
    }

    public <T extends Comparable> Collection<Container> tails(Collection<Container> collection,  SortOrderBy<T>... orderByList) {
        if (collection == null || collection.isEmpty()) {
            return null;
        } else {
            return collection.stream().map(FunctionRunner::containerHolder).collect(maxList(createComparatorFromSortOrderBys(orderByList))).stream().map(Holder::getValue).collect(Collectors.toList());
        }
    }

    private static Holder<Container> containerHolder(Container container) {
        return new Holder<>(container);
    }

    public boolean isAssignable(Container container, EClass targetClass) {
        boolean result = false;
        result = result || targetClass.equals(container.clazz);
        if (script.asmUtils.isMappedTransferObjectType(container.clazz) && script.asmUtils.isMappedTransferObjectType(targetClass)) {
            String entityTypeName = container.getPayload().getAs(String.class, ENTITY_TYPE);
            EClass sourceEntityType = script.asmUtils.getClassByFQName(entityTypeName).get();
            EClass targetEntityType = script.asmUtils.getMappedEntityType(targetClass).get();
            result = result || sourceEntityType.equals(targetEntityType);
            result = result || sourceEntityType.getEAllSuperTypes().contains(targetEntityType);
        }
        return result;
    }

    public Container getPrincipal(EClass actorType) {
        Container result = null;
        if (isCurrentActor(actorType)) {
            result = actorType.getEOperations().stream().filter(this::isGetPrincipalOperation).findAny().map(
                    eOperation -> {
                        Map<String, Object> map = script.dispatcher.callOperation(AsmUtils.getOperationFQName(eOperation), Payload.map(Dispatcher.PRINCIPAL_KEY, script.principal));
                        String outputParameterName = AsmUtils.getOutputParameterName(eOperation).get();
                        Payload payload = Payload.asPayload((Map<String, Object>) map.get(outputParameterName));
                        EClass toType = (EClass) eOperation.getEType();
                        payload.put(AbstractGeneratedScript.TO_TYPE, AsmUtils.getClassifierFQName(toType));
                        return script.createContainer(toType, payload);
                    }
            ).orElse(null);
        }
        return result;
    }

    public boolean  isCurrentActor(EClass actorType) {
        return script.principal != null && AsmUtils.getClassifierFQName(actorType).equals(script.principal.getClient());
    }

    private boolean isGetPrincipalOperation(EOperation eOperation) {
        return AsmUtils.getBehaviour(eOperation).filter(AsmUtils.OperationBehaviour.GET_PRINCIPAL::equals).isPresent();
    }

}
