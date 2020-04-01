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
    public static final String UNMAPPEDID = "__unmappedid";
    public static final String IDENTIFIER = "__identifier";
    public static final String TO_IDENTIFIER = "__to_identifier";
    public static final String MUTABLE_IDENTIFIER = "__mutable_identifier";

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
        if (container.uuid != null) {
            containers.get(container.uuid).values().forEach(set -> set.forEach(Container::delete));
            containers.remove(container.uuid);
        }
    }

    protected Container createImmutableContainer(Container container) {
        Payload newPayload = Payload.asPayload(container.payload);
        newPayload.put(UNMAPPEDID, UUID.randomUUID());
        if (newPayload.containsKey(IDENTIFIER)) {
            newPayload.put(MUTABLE_IDENTIFIER, newPayload.get(IDENTIFIER));
            newPayload.remove(IDENTIFIER);
        }
        Container newContainer = createContainer(container.clazz, newPayload);
        newContainer.immutable = true;
        return newContainer;
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
                String toType = p.getAs(String.class, "__toType");
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
            result = new Container(clazz, payload);
            Set<Container> existingContainers = containerMap.computeIfAbsent(className, k -> new HashSet<>());
            existingContainers.add(result);
            containerMap.put(className, existingContainers);
        }
        return result;
    }

    public class Container {
        public final Payload payload;
        public EClass clazz;
        public UUID uuid;
        public long lastRefresh = System.currentTimeMillis();
        public boolean deleted;
        public boolean immutable;

        public Container(EClass clazz, Payload payload) {
            this.clazz = clazz;
            this.payload = payload;
            this.uuid = (UUID) payload.get(idProvider.getName());
        }
        public Container() {
            payload = null;
            clazz = null;
            uuid = null;
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
                if (uuid != null) {
                    Payload newPayload = (Payload) dao.getByIdentifier(clazz, uuid).orElse(Payload.empty());
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
            dao.delete(container.clazz, container.uuid);
        }
        deleteContainer(container);
        write();
    }

}
