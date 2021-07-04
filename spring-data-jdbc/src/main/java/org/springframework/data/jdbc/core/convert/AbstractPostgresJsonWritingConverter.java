package org.springframework.data.jdbc.core.convert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.PGobject;
import org.springframework.core.convert.converter.Converter;
import java.sql.SQLException;

/**
 * An abstract class for building your own converter for PostgerSQL's JSON[b].
 *
 * @author Nikita Konev
 */
public abstract class AbstractPostgresJsonWritingConverter<T> implements Converter<T, PGobject> {
    private final ObjectMapper objectMapper;
    private final boolean jsonb;

    public AbstractPostgresJsonWritingConverter(ObjectMapper objectMapper, boolean jsonb) {
        this.objectMapper = objectMapper;
        this.jsonb = jsonb;
    }

    @Override
    public PGobject convert(T source) {
        try {
            final PGobject pGobject = new PGobject();
            pGobject.setType(jsonb ? "jsonb" : "json");
            pGobject.setValue(objectMapper.writeValueAsString(source));
            return pGobject;
        } catch (JsonProcessingException | SQLException e) {
            throw new RuntimeException("Unable to serialize to json " + source, e);
        }
    }
}
