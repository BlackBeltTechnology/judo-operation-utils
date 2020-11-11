package hu.blackbelt.judo.operation.utils;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.dispatcher.api.JudoPrincipal;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import org.eclipse.emf.ecore.*;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractGeneratedScript implements Function<Payload, Payload> {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AbstractGeneratedScript.class);
    protected static final String UNMAPPEDID = "__unmappedid";
    protected static final String IDENTIFIER = "__identifier";
    protected static final String TO_IDENTIFIER = "__to_identifier";
    protected static final String ENTITY_TYPE = "__entityType";
    /**
     * Set when the container is an immutable copy of an entity with IDENTIFIER
     */
    protected static final String MUTABLE_IDENTIFIER = "__mutable_identifier";
    /**
     * Set to the actual transfer object type when entity is looked up via 'select all' (e.g. demo::entities::Person) or created with new (e.g. new demo::entities::Person)
     */
    public static final String TO_TYPE = "__toType";

    protected AbstractGeneratedScript() {
        functionRunner = new FunctionRunner(this);
    }

    public static class Holder<T> {
        public T value;

        public Holder() {

        }

        public Holder(T value) {
            this.value = value;
        }

        public T getValue() {
            return value;
        }
    }

    public static class SortOrderBy<T extends Comparable> {
        public Function<Holder<Container>, T> generator;
        public boolean descending;

        public SortOrderBy(Function<Holder<Container>, T> generator, boolean descending) {
            this.generator = generator;
            this.descending = descending;
        }
    }

    protected DAO<UUID> dao;
    protected Dispatcher dispatcher;
    protected IdentifierProvider<UUID> idProvider;
    protected AsmModel asmModel;

    protected AsmUtils asmUtils;

    protected String outputName;
    protected int outputLowerBound;
    protected int outputUpperBound;
    protected JudoPrincipal principal;

    protected FunctionRunner functionRunner;

    public void setDao(DAO<UUID> dao) {
        this.dao = dao;
    }

    public void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void setIdProvider(IdentifierProvider<UUID> idProvider) {
        this.idProvider = idProvider;
    }

    public void setAsmModel(AsmModel asmModel) {
        this.asmModel = asmModel;
        this.asmUtils = new AsmUtils(asmModel.getResourceSet());
    }

    protected String getFqName(String namespace, String name) {
        return replaceSeparator(String.format("%s::%s", namespace, name));
    }

    protected String replaceSeparator(String fqName) {
        return fqName.replaceAll("::", ".");
    }

    protected long lastWrite = System.currentTimeMillis();

    protected void write() {
        lastWrite = System.currentTimeMillis();
    }

    protected boolean isMappedAttribute(EClass clazz, String name) {
        return clazz.getEAllAttributes().stream().filter(r -> Objects.equals(r.getName(), name))
                .findAny()
                .flatMap(asmUtils::getMappedAttribute)
                .isPresent()
                ;
    }

    protected boolean isMappedReference(EClass clazz, String name) {
        return clazz.getEAllReferences().stream().filter(r -> Objects.equals(r.getName(), name))
                .findAny()
                .flatMap(asmUtils::getMappedReference)
                .isPresent();
    }

    protected Map<UUID, Map<String, Set<Container>>> containers = new HashMap<>();
    protected Map<UUID, Container> unmappeds = new HashMap<>();

    protected void deleteContainer(Container container) {
        if (container.getId() != null) {
            containers.get(container.getId()).values().forEach(set -> set.forEach(Container::delete));
            containers.remove(container.getId());
        }
    }

    protected Container createMutableContainer(Container container) {
        Payload newPayload = Payload.asPayload(container.payload);
        if (newPayload.containsKey(MUTABLE_IDENTIFIER)) {
            newPayload.put(IDENTIFIER, newPayload.remove(MUTABLE_IDENTIFIER));
            newPayload.remove(UNMAPPEDID);
        }
        Container refreshedNewContainer = new Container(container.clazz, newPayload).refresh();
        Container result = createContainer(container.clazz, refreshedNewContainer.getPayload());
        result.deleted = refreshedNewContainer.deleted;
        return result;
    }

    protected Container createImmutableContainer(EClass clazz, Payload payload) {
        if (payload.containsKey(IDENTIFIER)) {
            payload.put(MUTABLE_IDENTIFIER, payload.remove(IDENTIFIER));
        }
        Container newContainer = createContainer(clazz, payload);
        newContainer.immutable = true;
        return newContainer;
    }

    protected Container createImmutableContainer(Container container) {
        Payload newPayload = Payload.asPayload(container.refresh().payload);
        return createImmutableContainer(container.clazz, newPayload);
    }

    protected Collection<Container> createImmutableContainerList(Collection<Container> containers) {
        List<Container> result = new ArrayList<>();
        for (Container container : containers) {
            result.add(createImmutableContainer(container));
        }
        return result;
    }

    protected Collection<Container> createMutableContainerList(Collection<Container> containers) {
        List<Container> result = new ArrayList<>();
        for (Container container : containers) {
            result.add(createMutableContainer(container));
        }
        return result;
    }

    protected Container createContainer(EClass clazz, Payload payload) {
        Container result;
        if (payload == null) {
            result = new Container();
        } else {
            if (payload.containsKey(IDENTIFIER)) {
                result = createOrReturnMappedContainer(clazz, payload);
            } else {
                result = createUnmappedOrImmutableContainer(clazz, payload);
            }
        }
        return result;
    }

    private Container createUnmappedOrImmutableContainer(EClass clazz, Payload payload) {
        Payload newPayload;
        if (!isMapped(clazz)) {
            newPayload = dao.getStaticFeatures(clazz);
            for (String key : payload.keySet()) {
                newPayload.put(key, payload.get(key));
            }
        } else {
            newPayload = payload;
        }
        Container result;
        UUID unmappedId;
        if (!newPayload.containsKey(UNMAPPEDID)) {
            unmappedId = UUID.randomUUID();
            newPayload.put(UNMAPPEDID, unmappedId);
        } else {
            unmappedId = newPayload.getAs(UUID.class, UNMAPPEDID);
        }
        result = new Container(clazz, newPayload);
        unmappeds.put(unmappedId, result);
        for (Map.Entry<String, Object> entry : newPayload.entrySet()) {
            Collection<Payload> payloadsToProcess;
            if (entry.getValue() instanceof Payload) {
                payloadsToProcess = Collections.singleton((Payload) entry.getValue());
            } else if (entry.getValue() instanceof Collection) {
                payloadsToProcess = newPayload.getAsCollectionPayload(entry.getKey());
            } else {
                payloadsToProcess = Collections.emptySet();
            }
            payloadsToProcess.forEach(p -> {
                if (newPayload.containsKey(MUTABLE_IDENTIFIER)) {
                    if (p.containsKey(IDENTIFIER)) {
                        p.put(MUTABLE_IDENTIFIER, p.remove(IDENTIFIER));
                    }
                }
                String toType = p.getAs(String.class, TO_TYPE);
                EClass toClass;
                if (toType == null) {
                    toClass = clazz.getEAllReferences().stream().filter(ref -> ref.getName().equals(entry.getKey())).findAny().map(ref -> ref.getEReferenceType()).orElse(null);
                    p.put(TO_TYPE, AsmUtils.getClassifierFQName(toClass));
                } else {
                    toClass = asmUtils.getClassByFQName(toType).orElse(null);
                }
                createContainer(toClass, p);
            });
        }
        return result;
    }

    /*
    Given a payload for a mapped transfer object it either creates a container for it or returns the one already managed.
     */
    private Container createOrReturnMappedContainer(EClass clazz, Payload payload) {
        Container result;
        String className = AsmUtils.getClassifierFQName(clazz);
        UUID id = (UUID) payload.get(IDENTIFIER);
        Map<String, Set<Container>> containerMap = containers.computeIfAbsent(id, k -> new HashMap<>());
        if (containerMap.containsKey(className)) {
            Set<Container> existingContainers = containerMap.get(className);
            existingContainers.forEach(c -> c.updatePayload(payload));
            Optional<Container> optionalResult = existingContainers.stream().filter(c -> payload.containsKey(TO_IDENTIFIER) && payload.getAs(UUID.class, TO_IDENTIFIER).equals(c.payload.getAs(UUID.class, TO_IDENTIFIER))).findAny();
            if (!optionalResult.isPresent()) {
                result = new Container(clazz, payload);
                existingContainers.add(result);
            } else {
                result = optionalResult.get();
            }
        } else {
            payload.put(TO_IDENTIFIER, UUID.randomUUID());
            payload.put(TO_TYPE, AsmUtils.getClassifierFQName(clazz));
            result = new Container(clazz, payload);
            Set<Container> existingContainers = containerMap.computeIfAbsent(className, k -> new LinkedHashSet<>());
            existingContainers.add(result);
            containerMap.put(className, existingContainers);
        }
        return result;
    }

    public Optional<? extends Collection<Container>> findContainers(Payload payload) {
        log.debug("Finding container " + payload);
        if (payload.containsKey(UNMAPPEDID)) {
            if (unmappeds.containsKey(payload.getAs(UUID.class, UNMAPPEDID))) {
                return Optional.of(Collections.singleton(unmappeds.get(payload.getAs(UUID.class, UNMAPPEDID))));
            }
        } else if (payload.containsKey(IDENTIFIER)) {
            Map<String, Set<Container>> map = containers.get(payload.getAs(UUID.class, IDENTIFIER));
            if (map != null) {
                return Optional.of(map.values().stream().flatMap(Collection::stream).collect(Collectors.toCollection(LinkedHashSet::new)));
            } else {
                return Optional.empty();
            }
        }
        return Optional.empty();
    }

    public class Container {
        public final Payload payload;
        public EClass clazz;
        public long lastRefresh = 0;
        public boolean deleted;
        public boolean immutable;

        public Container(EClass clazz, Payload payload) {
            this.clazz = clazz;
            this.payload = payload;
        }

        public Container() {
            payload = null;
            clazz = null;
        }

        public UUID getId() {
            UUID result = getMappedId();
            if (result == null) {
                result = payload.getAs(UUID.class, MUTABLE_IDENTIFIER);
            }
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            boolean result = false;
            if (obj instanceof Container) {
                Container other = (Container) obj;
                if (Objects.equals(getMappedId(), other.getMappedId())) {
                    result = true;
                } else {
                    result = super.equals(obj);
                }
            }
            return result;
        }

        public int hashCode() {
            if (getMappedId() != null) {
                return getMappedId().hashCode();
            } else {
                return super.hashCode();
            }
        }

        public UUID getMappedId() {
            return payload.getAs(UUID.class, IDENTIFIER);
        }

        public void delete() {
            deleted = true;
        }

        public void updatePayload(Payload newPayload) {
            if (immutable) {
                throw new IllegalStateException("Tried to refresh immutable container");
            }
            if (newPayload.isEmpty()) {
                payload.clear();
                return;
            }
            for (String key : newPayload.keySet()) {
                payload.put(key, newPayload.get(key));
            }
            clazz.getEAllAttributes().stream()
                    .map(ENamedElement::getName)
                    .filter(n -> isMappedAttribute(clazz, n))
                    .forEach(n -> {
                        if (!newPayload.containsKey(n)) {
                            payload.remove(n);
                        }
                    });
            clazz.getEAllReferences().stream()
                    .map(ENamedElement::getName)
                    .filter(n -> isMappedReference(clazz, n))
                    .forEach(n -> {
                        if (!newPayload.containsKey(n)) {
                            payload.remove(n);
                        }
                    });
            lastRefresh = System.currentTimeMillis();
            log.debug(String.format("Payload %s merged as %s", newPayload, payload));
        }

        public Container refresh() {
            if (!immutable && lastWrite >= lastRefresh) {
                lastRefresh = System.currentTimeMillis();
                if (getMappedId() != null) {
                    Payload newPayload = (Payload) dao.getByIdentifier(clazz, getMappedId()).orElseGet(() -> {
                        deleted = true;
                        return Payload.empty();
                    });
                    updatePayload(newPayload);
                } else {
                    Set<String> removableKeys = new LinkedHashSet<>();
                    for (Map.Entry<String, Object> entry : payload.entrySet()) {
                        if (entry.getValue() instanceof Payload) {
                            Payload contentPayload = (Payload) entry.getValue();
                            Optional<? extends Collection<Container>> optContainer = findContainers(contentPayload);
                            if (optContainer.isPresent()) {
                                optContainer.get().forEach(container -> {
                                    container.refresh();
                                    if (container.payload.isEmpty()) {
                                        removableKeys.add(entry.getKey());
                                    }
                                });
                            } else {
                                if (contentPayload.containsKey(UNMAPPEDID) || contentPayload.containsKey("__identifier")) {
                                    removableKeys.add(entry.getKey());
                                }
                            }
                        } else if (entry.getValue() instanceof Collection) {
                            Collection contentCollection = new ArrayList((Collection) entry.getValue());
                            List<Payload> removable = new ArrayList<>();
                            for (Object elem : contentCollection) {
                                if (elem instanceof Payload) {
                                    Payload payloadElem = (Payload) elem;
                                    Optional<? extends Collection<Container>> optContainer = findContainers(payloadElem);
                                    if (optContainer.isPresent()) {
                                        optContainer.get().forEach(container -> {
                                            container.refresh();
                                            if (container.payload.isEmpty()) {
                                                removable.add(payloadElem);
                                            }
                                        });
                                    } else {
                                        if (payloadElem.containsKey(UNMAPPEDID) || payloadElem.containsKey("__identifier")) {
                                            removable.add(payloadElem);
                                        }
                                    }
                                }
                            }
                            contentCollection.removeAll(removable);
                            if (contentCollection.isEmpty()) {
                                removableKeys.add(entry.getKey());
                            }
                        }
                    }
                    removableKeys.stream()
                            .filter(key -> payload.get(key) instanceof Collection)
                            .map(payload::get)
                            .forEach(c -> ((Collection) c).clear());
                    payload.keySet().removeAll(removableKeys.stream()
                            .filter(key -> !(payload.get(key) instanceof Collection))
                            .collect(Collectors.toList()));
                }
            }
            return this;
        }

        public Payload getPayload() {
            if (deleted) {
                return Payload.empty();
            } else {
                return refresh().payload;
            }
        }
    }

    public boolean isMapped(EClass eClass) {
        return eClass != null && asmUtils.isMappedTransferObjectType(eClass);
    }

    protected void deleteElement(Container container) {
        if (container != null) {
            if (isMapped(container.clazz)) {
                dao.delete(container.clazz, container.getId());
            }
            deleteContainer(container);
            write();
        }
    }

    protected Collection<Container> containersForAll(String namespace, String name) {
        String fqName = getFqName(namespace, name);
        List<Container> result = new ArrayList<>();
        EClass clazz = asmUtils.getClassByFQName(fqName).get();
        List<Payload> payloads = dao.getAllOf(clazz);
        for (Payload payload : payloads) {
            payload.put(TO_TYPE, AsmUtils.getClassifierFQName(clazz));
            result.add(createContainer(clazz, payload));
        }
        return result;
    }

    protected void updateAttribute(Container container, String attributeName, Object assignableObject) {
        Payload instance;
        if (container != null) {
            if (isMapped(container.clazz)) {
                instance = (Payload) dao.getByIdentifier(container.clazz, container.getId()).get();
            } else {
                instance = container.getPayload();
            }
            instance.put(attributeName, assignableObject);
            if (isMapped(container.clazz)) {
                if (isMappedAttribute(container.clazz, attributeName)) {
                    container.updatePayload(dao.update(container.clazz, instance));
                } else {
                    container.updatePayload(instance);
                }
            }
            write();
        }
    }

    protected Set<Container> containersFromNavigation(Collection<Container> containers, String referenceName) {
        Set<Container> result = new LinkedHashSet<>();
        for (Container container : containers) {
            result.addAll(containersFromNavigation(container, referenceName));
        }
        return result;
    }

    protected Set<Container> containersFromNavigation(Container container, String referenceName) {
        Set<Container> result = new LinkedHashSet<>();
        if (container != null) {
            EReference ref = container.clazz.getEAllReferences().stream().filter(r -> Objects.equals(r.getName(), referenceName)).findAny().get();
            List<Payload> payloads;
            if (isMapped(container.clazz) && isMappedReference(container.clazz, ref.getName())) {
                payloads = dao.getNavigationResultAt(container.getId(), ref);
            } else {
                payloads = container.getPayload().containsKey(referenceName) ?
                        new ArrayList<>((Collection) container.getPayload().get(referenceName)) :
                        new ArrayList<>();
            }
            for (Payload payload : payloads) {
                result.add(createContainer(ref.getEReferenceType(), payload));
            }
        }
        return result;
    }

    protected Container spawnContainer(EClass clazz, Container original) {
        UUID mappedId = original.getMappedId();
        if (mappedId == null) {
            throw new IllegalArgumentException("Entity id is null");
        }
        Optional payloadByIdentifier = dao.getByIdentifier(clazz, mappedId);
        Container container;
        if (payloadByIdentifier.isPresent()) {
             container = createContainer(clazz, (Payload) payloadByIdentifier.get());
        } else {
            container = null;
        }
        write();
        return container;
    }

    @Override
    public Payload apply(Payload exchange) {
        log.debug("Operation called with exchange: " + exchange);
        if (exchange != null) {
            this.principal = exchange.getAs(JudoPrincipal.class, Dispatcher.PRINCIPAL_KEY);
        }
        Holder<Payload> outputHolder = new Holder<>();
        doApply(exchange, outputHolder);
        if (this.outputName != null) {
            if (outputHolder.value == null) {
                if (this.outputLowerBound != 0) {
                    throw new RuntimeException("Script output is not set (missing return statement).");
                } else {
                    if (this.outputUpperBound == 1) {
                        outputHolder.value = Payload.map(this.outputName, Payload.empty());
                    } else {
                        outputHolder.value = Payload.map(this.outputName, new LinkedHashSet<Payload>());
                    }
                }

            }
            return outputHolder.value;
        } else {
            return null;
        }
    }

    protected Integer getEnumOrdinal(String enumNamespace, String enumName, String enumValue) {
        String enumFqName = replaceSeparator(getFqName(enumNamespace, enumName));
        EEnum eEnum = (EEnum) asmUtils.resolve(enumFqName).get();
        return eEnum.getEEnumLiteral(enumValue).getValue();
    }

    protected Payload getStaticData(String namespace, String name, String attributeName) {
        String typeFqName = replaceSeparator(getFqName(namespace, name));
        EAttribute eAttribute = asmUtils.getClassByFQName(typeFqName).get().getEAttributes().stream().filter(attr -> attr.getName().equals(attributeName)).findAny().get();
        return dao.getStaticData(eAttribute);
    }

    protected Container getStaticDataContainer(String namespace, String name) {
        String typeFqName = replaceSeparator(getFqName(namespace, name));
        EClass eClass = asmUtils.getClassByFQName(typeFqName).get();
        return createUnmappedOrImmutableContainer(eClass, Payload.empty());
    }

    protected boolean isContainment(EReference reference) {
        return asmUtils.getMappedReference(reference).map(EReference::isContainment).get();
    }

    protected void assignNewEmbeddedCollection(Container target, EReference reference, Collection<Payload> payloads) {
        List<Payload> currentContent = dao.getNavigationResultAt(target.getId(), reference);
        // delete all content, except when readded
        Set<UUID> addedIds = payloads.stream().filter(payload -> payload.containsKey(IDENTIFIER)).map(payload -> payload.getAs(UUID.class, IDENTIFIER)).collect(Collectors.toSet());
        currentContent.stream().filter(payload -> !addedIds.contains(payload.getAs(UUID.class, IDENTIFIER))).forEach(
                payload -> dao.deleteNavigationInstanceAt(target.getId(), reference, payload));
        List<UUID> currentIds = currentContent.stream().map(payload -> payload.getAs(UUID.class, IDENTIFIER)).collect(Collectors.toList());
        payloads.stream().filter(payload -> !currentIds.contains(payload.getAs(UUID.class, IDENTIFIER))).forEach( payload -> {
            if (!payload.containsKey(IDENTIFIER)) {
                dao.createNavigationInstanceAt(target.getId(), reference, payload);
            } else {
                dao.addReferences(reference, target.getId(), Collections.singleton(payload.getAs(UUID.class, IDENTIFIER)));
            }
            write();
        });
    }

    protected abstract void doApply(Payload exchange, Holder<Payload> outputHolder);

}
