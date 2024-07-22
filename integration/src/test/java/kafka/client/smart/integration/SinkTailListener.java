/*-
 * Copyright (C) 2022-2024 Confluent, Inc.
 */

package kafka.client.smart.integration;

import lombok.Getter;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SinkTailListener implements TailerListener {

    private final Pattern pattern = Pattern.compile("([A-Za-z]*=[A-Za-z0-9_@.]*)+");

    @Getter
    private final List<Object> lines = new ArrayList<>();

    @Getter
    private final CountDownLatch latch;

    private final int maxLines;
    private Tailer tailer;

    public SinkTailListener(int maxLines) {
        latch = new CountDownLatch(maxLines);
        this.maxLines = maxLines;
    }

    @Override
    public void init(Tailer tailer) {
        this.tailer = tailer;
    }

    @Override
    public void fileNotFound() {
        tailer.stop();
    }

    @Override
    public void fileRotated() {
    }

    @Override
    public void handle(String s) {
        if (lines.size() == maxLines) {
            tailer.stop();
            return;
        }

        if (s.startsWith("Struct{")) {
            final Map<String, String> values = new HashMap<>();
            final Matcher matcher = pattern.matcher(s);
            while (matcher.find()) {
                final String value = matcher.group();
                final String[] elements = value.split("=");
                values.put(elements[0], elements[1]);
            }
            lines.add(values);
        } else {
            lines.add(s);
        }

        latch.countDown();
    }

    @Override
    public void handle(Exception e) {
        tailer.stop();
    }
}
