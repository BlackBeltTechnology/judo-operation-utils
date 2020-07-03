package hu.blackbelt.judo.operation.utils;

import hu.blackbelt.judo.dao.api.DAO;
import hu.blackbelt.judo.dao.api.IdentifierProvider;
import hu.blackbelt.judo.dao.api.Payload;
import hu.blackbelt.judo.dispatcher.api.Dispatcher;
import hu.blackbelt.judo.meta.asm.runtime.AsmModel;
import hu.blackbelt.judo.meta.asm.runtime.AsmUtils;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.ENamedElement;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public abstract class AbstractGeneratedScript implements Function<Payload, Payload> {

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(AbstractGeneratedScript.class);
    protected static final String UNMAPPEDID = "__unmappedid";
    protected static final String IDENTIFIER = "__identifier";
    protected static final String TO_IDENTIFIER = "__to_identifier";
    /** Set when the container is an immutable copy of an entity with IDENTIFIER */
    protected static final String MUTABLE_IDENTIFIER = "__mutable_identifier";
    /** Set to the actual transfer object type when entity is looked up via 'select all' (e.g. demo::entities::Person) or created with new (e.g. new demo::entities::Person) */
    public static final String TO_TYPE = "__toType";

    public static class Holder<T> {
        public T value;
    }

    protected DAO dao;
    protected Dispatcher dispatcher;
    protected IdentifierProvider<UUID> idProvider;
    protected AsmModel asmModel;

    protected AsmUtils asmUtils;

    protected String outputName;
    protected int outputLowerBound;
    protected int outputUpperBound;

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

    protected List<Container> createImmutableContainerList(List<Container> containers) {
        List<Container> result = new ArrayList<>();
        for (Container container : containers) {
            result.add(createImmutableContainer(container));
        }
        return result;
    }

    protected List<Container> createMutableContainerList(List<Container> containers) {
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
                result = createUnmappedContainer(clazz, payload);
            }
        }
        return result;
    }

    private Container createUnmappedContainer(EClass clazz, Payload payload) {
        Container result;
        UUID unmappedId;
        if (!payload.containsKey(UNMAPPEDID)) {
            unmappedId = UUID.randomUUID();
            payload.put(UNMAPPEDID, unmappedId);
        } else {
            unmappedId = payload.getAs(UUID.class, UNMAPPEDID);
        }
        result = new Container(clazz, payload);
        unmappeds.put(unmappedId, result);
        for (Map.Entry<String, Object> entry : payload.entrySet()) {
            Collection<Payload> payloadsToProcess;
            if (entry.getValue() instanceof Payload) {
                payloadsToProcess = Collections.singleton((Payload) entry.getValue());
            } else if (entry.getValue() instanceof Collection) {
                payloadsToProcess = payload.getAsCollectionPayload(entry.getKey());
            } else {
                payloadsToProcess = Collections.emptySet();
            }
            payloadsToProcess.forEach(p -> {
                if (payload.containsKey(MUTABLE_IDENTIFIER)) {
                    if (p.containsKey(IDENTIFIER)) {
                        p.put(MUTABLE_IDENTIFIER, p.remove(IDENTIFIER));
                    }
                }
                String toType = p.getAs(String.class, TO_TYPE);
                createContainer(toType == null ? null : asmUtils.getClassByFQName(toType).orElse(null), p);
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
            Set<Container> existingContainers = containerMap.computeIfAbsent(className, k -> new HashSet<>());
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
                return Optional.of(map.values().stream().flatMap(Collection::stream).collect(Collectors.toSet()));
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
                    .forEach(n -> { if (!newPayload.containsKey(n)) { payload.remove(n); } });
            clazz.getEAllReferences().stream()
                    .map(ENamedElement::getName)
                    .filter(n -> isMappedReference(clazz, n))
                    .forEach(n -> { if (!newPayload.containsKey(n)) { payload.remove(n); } });
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
                    Set<String> removableKeys = new HashSet<>();
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
        if (isMapped(container.clazz)) {
            dao.delete(container.clazz, container.getId());
        }
        deleteContainer(container);
        write();
    }

    protected List<Container> containersForAll(String namespace, String name) {
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

    @Override
    public Payload apply(Payload exchange) {
        log.debug("Operation called with exchange: " + exchange);
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
                        outputHolder.value = Payload.map(this.outputName, new HashSet<Payload>());
                    }
                }

            }
            return outputHolder.value;
        } else {
            return null;
        }
    }

    protected abstract void doApply(Payload exchange, Holder<Payload> outputHolder);


}
