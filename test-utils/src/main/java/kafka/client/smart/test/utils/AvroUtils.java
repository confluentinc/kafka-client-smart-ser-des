/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.smart.test.utils;

import com.google.common.base.Charsets;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

@Slf4j
@SuppressWarnings({"unused"})
public class AvroUtils {

    private AvroUtils() {}

    // Using chained resolve, we don't need to worry about path separators
    static final Path AVRO_DIR_RESOLVED =
            Paths.get("").toAbsolutePath().resolve("src").resolve("test").resolve("avro");

    @SneakyThrows
    public static InputStream getAvroSourceStream(String sourceFileName) {
        Path filePath = getAvroPath(sourceFileName);
        File file = new File(filePath.toUri());
        return new FileInputStream(file);
    }

    //    @javax.validation.constraints.NotNull
    private static Path getAvroPath(String sourceFileName) {
        int slashIndex = sourceFileName.indexOf("/");

        // chomp leading slash
        if (slashIndex == 0) {
            sourceFileName = sourceFileName.substring(slashIndex + 1);
        }

        return AVRO_DIR_RESOLVED.resolve(sourceFileName);
    }

    @SneakyThrows
    public static String getAvroSource(String sourceFileName) {
        Path avroPath = getAvroPath(sourceFileName);
        File file = new File(avroPath.toUri());
        return com.google.common.io.Files.asCharSource(file, Charsets.UTF_8).read();
    }

    @SuppressWarnings("unchecked")
    public static <T> T toAVRO(final Map<String, Object> objectMap, final Class<?> tClass)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
            InstantiationException {

        final T tInstance = (T) tClass.getConstructor().newInstance();

        objectMap.forEach(
                (k, v) -> {
                    try {
                        final Field field = tClass.getDeclaredField(k);
                        field.setAccessible(true);

                        Object vInstance;

                        if (v instanceof Map) {
                            final Map<String, Object> values = (Map<String, Object>) v;
                            if (values.size() == 1) {
                                // Value type
                                vInstance = mapHandler(tClass, k, values);

                                field.set(tInstance, vInstance);
                            } else {
                                Class<?> declaringClass = field.getType();
                                Object instance = toAVRO(values, declaringClass);

                                field.set(tInstance, instance);
                            }
                        } else if (v instanceof ArrayList) {
                            vInstance = listHandler(k, (ArrayList<?>) v);
                            field.set(tInstance, vInstance);
                        } else {
                            field.set(tInstance, v);
                        }
                    } catch (NoSuchFieldException
                             | ClassNotFoundException
                             | IllegalAccessException
                             | NoSuchMethodException
                             | InvocationTargetException
                             | InstantiationException exception) {
                        log.error("Error: ", exception);
                    }
                });

        return tInstance;
    }

    @NotNull
    private static List<Object> listHandler(String k, ArrayList<?> arrayList)
            throws NoSuchFieldException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException, ClassNotFoundException {
        List<Object> theList = new ArrayList<>();

        for (Object value : arrayList) {
            Class<?> valueType = value.getClass();
            if (valueType.isAssignableFrom(String.class)) {
                theList.add(value);
            } else if (value instanceof Map) {
                Object valueInstance = mapHandler(valueType, k, (Map<String, Object>) value);
                theList.add(valueInstance);
            } else {
                Object valueInstance = toAVRO((Map<String, Object>) value, valueType);
                theList.add(valueInstance);
            }
        }
        return theList;
    }

    private static Object mapHandler(Class<?> tClass, String k, Map<String, Object> values)
            throws NoSuchFieldException, NoSuchMethodException, IllegalAccessException,
            InvocationTargetException, InstantiationException, ClassNotFoundException {
        Object vInstance;
        final Map.Entry<String, Object> entry = values.entrySet().stream().findFirst().orElse(null);
        switch (entry.getKey()) {
            case "double":
            case "string":
                vInstance = entry.getValue();
                break;
            case "array":
                ArrayList<?> arrayList = (ArrayList<?>) entry.getValue();
                try {
                    Field arrayField;
                    try {
                        arrayField = tClass.getDeclaredField(k);
                    } catch (NoSuchFieldException ignore) {
                        arrayField = tClass.getField(k);
                    }

                    final Type genericType = arrayField.getGenericType();

                    if (genericType instanceof ParameterizedType) {
                        ParameterizedType parameterizedType = (ParameterizedType) genericType;
                        Class<?> valueType = (Class<?>) parameterizedType.getActualTypeArguments()[0];

                        List<Object> theList = new ArrayList<>();

                        for (Object value : arrayList) {
                            if (value instanceof String) {
                                theList.add(value);
                            } else {
                                Object valueInstance = toAVRO((Map<String, Object>) value, valueType);
                                theList.add(valueInstance);
                            }
                        }

                        vInstance = theList;

                    } else {
                        vInstance = entry.getValue();
                    }
                } catch (NoSuchFieldException ignore) {
                    vInstance = entry.getValue();
                }
                break;
            default:
                try {
                    Class<?> sType = Class.forName(entry.getKey());
                    vInstance = toAVRO((Map<String, Object>) entry.getValue(), sType);
                } catch (ClassNotFoundException e) {
                    return entry.getValue();
                }
                break;
        }
        return vInstance;
    }
}