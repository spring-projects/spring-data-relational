package org.springframework.data.jdbc.core.dialect;

import org.h2.api.TimestampWithTimeZone;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;

/**
 * Converter converting from an H2 internal representation of a timestamp with time zone to an {@link java.time.ZonedDateTime}.
 *
 * Only required for H2 versions < 2.0
 *
 * @author Mikhail Polivakha
 */
@ReadingConverter
public enum H2TimestampWithTimeZoneToZonedDateTimeConverter implements Converter<TimestampWithTimeZone, ZonedDateTime> {

    INSTANCE;

    @Override
    public ZonedDateTime convert(TimestampWithTimeZone source) {

        long nanosInSecond = 1_000_000_000;
        long nanosInMinute = nanosInSecond * 60;
        long nanosInHour = nanosInMinute * 60;

        long hours = (source.getNanosSinceMidnight() / nanosInHour);

        long nanosInHours = hours * nanosInHour;
        long nanosLeft = source.getNanosSinceMidnight() - nanosInHours;
        long minutes = nanosLeft / nanosInMinute;

        long nanosInMinutes = minutes * nanosInMinute;
        nanosLeft -= nanosInMinutes;
        long seconds = nanosLeft / nanosInSecond;

        long nanosInSeconds = seconds * nanosInSecond;
        nanosLeft -= nanosInSeconds;
        ZoneOffset offset = ZoneOffset.ofTotalSeconds(source.getTimeZoneOffsetSeconds());

        return ZonedDateTime.of(source.getYear(), source.getMonth(), source.getDay(), (int) hours, (int) minutes,
                (int) seconds, (int) nanosLeft, offset);
    }
}
