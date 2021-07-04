package org.springframework.data.jdbc.core.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.PGobject;
import org.springframework.core.convert.converter.Converter;

/**
 * An abstract class for building your own converter for PostgerSQL's JSON[b].
 *
 * @author Nikita Konev
 */
public abstract class AbstractPostgresJsonReadingConverter<T> implements Converter<PGobject, T> {
    private final ObjectMapper objectMapper;
    private final Class<T> valueType;

    public AbstractPostgresJsonReadingConverter(ObjectMapper objectMapper, Class<T> valueType) {
        this.objectMapper = objectMapper;
        this.valueType = valueType;
    }

    @Override
    public T convert(PGobject pgObject) {
        try {
            final String source = pgObject.getValue();
            return objectMapper.readValue(source, valueType);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Unable to deserialize to json " + pgObject, e);
        }
    }
}
