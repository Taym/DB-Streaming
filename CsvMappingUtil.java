import java.lang.reflect.Field;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CsvMappingUtil {

    public static <T> String mapObjectToCsvEntry(final Object object,final Class<T> clazz){
        if (Objects.isNull(object)){
            return "NULL";
        }
        else if (clazz.isAssignableFrom(ZonedDateTime.class)){
            final ZonedDateTime zdt = (ZonedDateTime) object;
            final String stringZdt = DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(zdt);
            return stringZdt;
        }
        else if(clazz.isAssignableFrom(Map.class)){
            final String json = ConfiguredObjectMapper.writeValueAsString(object);
            return json;
        }
        else{
            return object.toString();
        }
    }

    public static <T> List<String> mapObjectToCsvRecord(final T object,final Class<T> clazz){
        final Field[] fields = clazz.getDeclaredFields();
        final List<String> csvRecord = Stream.of(fields)
                .map(f -> {
                    try {
                        f.setAccessible(true);
                        return mapObjectToCsvEntry(f.get(object),f.getType());
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
        return csvRecord;
    }
}