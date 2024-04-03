package org.springframework.data.jdbc.core.convert;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.jdbc.core.mapping.AggregateReference;
import org.springframework.data.mapping.*;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.mapping.model.ParameterValueProvider;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Spring {@link Converter} to create instances of the given DTO type from the source value handed into the conversion
 * while also handling Aggregate References
 *
 * @author Mark Paluch
 * @author Oliver Drotbohm
 * @author Paul Jones
 * @since 3.3
 */
public class JdbcDtoInstantiatingConverter implements Converter<Object, Object> {

    private final Class<?> targetType;
    private final MappingContext<? extends PersistentEntity<?, ?>, ? extends PersistentProperty<?>> context;
    private final EntityInstantiator instantiator;

    /**
     * Create a new {@link Converter} to instantiate DTOs.
     *
     * @param dtoType must not be {@literal null}.
     * @param context must not be {@literal null}.
     * @param instantiators must not be {@literal null}.
     */
    public JdbcDtoInstantiatingConverter(Class<?> dtoType,
                                     MappingContext<? extends PersistentEntity<?, ?>, ? extends PersistentProperty<?>> context,
                                     EntityInstantiators instantiators) {

        Assert.notNull(dtoType, "DTO type must not be null");
        Assert.notNull(context, "MappingContext must not be null");
        Assert.notNull(instantiators, "EntityInstantiators must not be null");

        this.targetType = dtoType;
        this.context = context;
        this.instantiator = instantiators.getInstantiatorFor(context.getRequiredPersistentEntity(dtoType));
    }

    @NonNull
    @Override
    public Object convert(Object source) {

        if (targetType.isInterface()) {
            return source;
        }

        PersistentEntity<?, ? extends PersistentProperty<?>> sourceEntity = context
                .getRequiredPersistentEntity(source.getClass());
        PersistentPropertyAccessor<Object> sourceAccessor = sourceEntity.getPropertyAccessor(source);
        PersistentEntity<?, ? extends PersistentProperty<?>> targetEntity = context.getRequiredPersistentEntity(targetType);

        @SuppressWarnings({ "rawtypes", "unchecked" })
        Object dto = instantiator.createInstance(targetEntity, new ParameterValueProvider() {

            @Override
            @Nullable
            public Object getParameterValue(Parameter parameter) {

                String name = parameter.getName();

                if (name == null) {
                    throw new IllegalArgumentException(String.format("Parameter %s does not have a name", parameter));
                }

                return sourceAccessor.getProperty(sourceEntity.getRequiredPersistentProperty(name));
            }
        });

        PersistentPropertyAccessor<Object> targetAccessor = targetEntity.getPropertyAccessor(dto);
        InstanceCreatorMetadata<? extends PersistentProperty<?>> creator = targetEntity.getInstanceCreatorMetadata();

        targetEntity.doWithProperties((SimplePropertyHandler) property -> {

            if ((creator != null) && creator.isCreatorParameter(property)) {
                return;
            }

            targetAccessor.setProperty(property,
                    sourceAccessor.getProperty(sourceEntity.getRequiredPersistentProperty(property.getName())));
        });

        targetEntity.doWithAssociations((SimpleAssociationHandler) property -> {
            if ((creator != null) && creator.isCreatorParameter(property.getInverse())) {
                return;
            }

            if(property.getInverse().getType().equals(AggregateReference.class)) {
                targetAccessor.setProperty(property.getInverse(),
                        sourceAccessor.getProperty(sourceEntity.getRequiredPersistentProperty(property.getInverse().getName())));

            }
        });

        return dto;
    }
}