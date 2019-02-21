package org.springframework.data.r2dbc.repository.query;

import io.r2dbc.spi.Row;
import org.junit.Test;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.r2dbc.repository.query.RowDataConverter.RowToBooleanConverter;
import org.springframework.data.r2dbc.repository.query.RowDataConverter.RowToLocalDateConverter;
import org.springframework.data.r2dbc.repository.query.RowDataConverter.RowToLocalDateTimeConverter;
import org.springframework.data.r2dbc.repository.query.RowDataConverter.RowToLocalTimeConverter;
import org.springframework.data.r2dbc.repository.query.RowDataConverter.RowToNumberConverterFactory;
import org.springframework.data.r2dbc.repository.query.RowDataConverter.RowToNumberConverterFactory.RowToOffsetDateTimeConverter;
import org.springframework.data.r2dbc.repository.query.RowDataConverter.RowToNumberConverterFactory.RowToStringConverter;
import org.springframework.data.r2dbc.repository.query.RowDataConverter.RowToNumberConverterFactory.RowToUuidConverter;
import org.springframework.data.r2dbc.repository.query.RowDataConverter.RowToNumberConverterFactory.RowToZonedDateTimeConverter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowDataConverterTests {
    private static final int TOTAL_REGISTERED_CONVERTERS = 8;

    @Test
    public void isReturningAllCreatedConverts() {
        assertThat(RowDataConverter.getConvertersToRegister().size())
            .isEqualTo(TOTAL_REGISTERED_CONVERTERS);
    }

    @Test
    public void isConvertingBoolean() {
        Row row = mock(Row.class);
        when(row.get(0, Boolean.class)).thenReturn(true);

        assertTrue(RowToBooleanConverter.INSTANCE.convert(row));
    }

    @Test
    public void isConvertingLocalDate() {
        LocalDate now = LocalDate.now();
        Row row = mock(Row.class);
        when(row.get(0, LocalDate.class)).thenReturn(now);

        assertThat(RowToLocalDateConverter.INSTANCE.convert(row)).isEqualTo(now);
    }

    @Test
    public void isConvertingLocalDateTime() {
        LocalDateTime now = LocalDateTime.now();
        Row row = mock(Row.class);
        when(row.get(0, LocalDateTime.class)).thenReturn(now);

        assertThat(RowToLocalDateTimeConverter.INSTANCE.convert(row)).isEqualTo(now);
    }

    @Test
    public void isConvertingLocalTime() {
        LocalTime now = LocalTime.now();
        Row row = mock(Row.class);
        when(row.get(0, LocalTime.class)).thenReturn(now);

        assertThat(RowToLocalTimeConverter.INSTANCE.convert(row)).isEqualTo(now);
    }

    @Test
    public void isConvertingOffsetDateTime() {
        OffsetDateTime now = OffsetDateTime.now();
        Row row = mock(Row.class);
        when(row.get(0, OffsetDateTime.class)).thenReturn(now);

        assertThat(RowToOffsetDateTimeConverter.INSTANCE.convert(row)).isEqualTo(now);
    }

    @Test
    public void isConvertingString() {
        String value = "aValue";
        Row row = mock(Row.class);
        when(row.get(0, String.class)).thenReturn(value);

        assertThat(RowToStringConverter.INSTANCE.convert(row)).isEqualTo(value);
    }

    @Test
    public void isConvertingUUID() {
        UUID value = UUID.randomUUID();
        Row row = mock(Row.class);
        when(row.get(0, UUID.class)).thenReturn(value);

        assertThat(RowToUuidConverter.INSTANCE.convert(row)).isEqualTo(value);
    }

    @Test
    public void isConvertingZonedDateTime() {
        ZonedDateTime now = ZonedDateTime.now();
        Row row = mock(Row.class);
        when(row.get(0, ZonedDateTime.class)).thenReturn(now);

        assertThat(RowToZonedDateTimeConverter.INSTANCE.convert(row)).isEqualTo(now);
    }

    @Test
    public void isConvertingNumber() {
        Row row = mock(Row.class);
        when(row.get(0, Integer.class)).thenReturn(33);

        final Converter<Row, Integer> converter = RowToNumberConverterFactory.INSTANCE.getConverter(Integer.class);

        assertThat(converter.convert(row)).isEqualTo(33);
    }

    @Test
    public void isRaisingExceptionForInvalidNumber() {
        assertThatIllegalArgumentException().isThrownBy(
            () -> RowToNumberConverterFactory.INSTANCE.getConverter(null)
        );
    }
}
