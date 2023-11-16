package com.coralocean.writer;

import cn.hutool.core.util.StrUtil;
import com.alibaba.excel.annotation.ExcelIgnore;
import com.alibaba.excel.annotation.ExcelProperty;
import com.alibaba.fastjson.JSONObject;
import com.easyhome.settlement.excel.Chunkable;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;


@Slf4j
public class PrintWriterCsvWriter {


    public static void writeArray(String savePath, List<String[]> list) {
        log.info("list len = {}", list.size());
        try (PrintWriter printWriter = new PrintWriter(new BufferedWriter(new OutputStreamWriter(Files.newOutputStream(Paths.get(savePath), StandardOpenOption.CREATE, StandardOpenOption.APPEND), StandardCharsets.UTF_8), 8192), true)) {
            StringBuilder stringBuilder = new StringBuilder();
            int count = 0;
            for (String[] element : list) {
                String row = convertRow(element);
                stringBuilder.append(row).append("\n");
                count++;
                if (count == 1000) {
                    printWriter.print(stringBuilder);
                    stringBuilder.delete(0, stringBuilder.length());
                    stringBuilder.ensureCapacity(1000);
                    count = 0;
                }

            }
            if (count > 0) {
                printWriter.print(stringBuilder);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public static <T> void write(String savePath, List<T> list) {
        if (null == list || list.isEmpty()) return;
        Class<?> aClass = list.get(0).getClass();
        if (Chunkable.class.isAssignableFrom(aClass)) {
            Map<String, Integer> head = getHead(aClass);
            List<String[]> result = new ArrayList<>();
            for (T data : list) {
                result.add(convert(data, head));
            }
            writeArray(savePath, result);
        } else if (List.class.isAssignableFrom(aClass)) {
            List<String[]> result = new ArrayList<>();
            for (T data : list) {
                List<?> lines = (List<?>) data;
                String[] array = lines.stream().map(Object::toString).toArray(String[]::new);
                result.add(array);
            }
            writeArray(savePath, result);
        }

    }

    private static <T> String[] convert(T data, Map<String, Integer> head) {
        JSONObject object = JSONObject.parseObject(JSONObject.toJSONString(data));
        String[] array = new String[head.size()];
        for (Map.Entry<String, Integer> colEntry : head.entrySet()) {
            String key = colEntry.getKey().split("#%")[1];
            String colValue = object.getString(key);
            if (null == colValue || "null".equals(colValue)) {
                colValue = "";
            }
            colValue = colValue.trim().replace("\r\n", "");
            array[colEntry.getValue()] = colValue;
        }
        return array;
    }

    private static <T> Map<String, Integer> getHead(Class<T> aClass) {
        Field[] fields = aClass.getDeclaredFields();
        Map<String, List<String>> duplicatedFields = new TreeMap<>();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (!field.isAnnotationPresent(ExcelProperty.class) || field.isAnnotationPresent(ExcelIgnore.class)) {
                continue;
            }
            ExcelProperty property = field.getAnnotation(ExcelProperty.class);
            String colName = StrUtil.isBlank(property.value()[0]) ? field.getName() : property.value()[0];
            int colIndex = property.index() == -1 ? i : property.index();
            String index = String.valueOf(colIndex);
            duplicatedFields.putIfAbsent(index, new ArrayList<>());
            duplicatedFields.get(index).add(colName + "#%" + field.getName());
        }
        Map<String, Integer> result = new HashMap<>();
        int offset = 0;
        for (String index : duplicatedFields.keySet()) {
            List<String> duplicatedColNames = duplicatedFields.get(index);
            for (String duplicatedColName : duplicatedColNames) {
                result.put(duplicatedColName, offset++);
            }
        }
        return result;
    }


    private static String convertRow(String[] values) {
        return String.join(",", values);
    }


}
