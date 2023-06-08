/*-
 * Copyright (C) 2022-2023 Confluent, Inc.
 */

package csid.client.connect.tests;

import lombok.Getter;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SinkTailListener implements TailerListener {

    public class Employee {
        public String id;
        public String name;
        public int age;
        public int ssn;
        public String email;
    }

    private final Pattern pattern = Pattern.compile("Struct\\{ID=(emp_[0-9]{3}),Name=(user_[0-9]{3}),Age=([2-9][0-9]),SSN=([0-9]{7}),Email=([a-z]{4}\\@mycompany\\.com)}");

    @Getter
    private final List<Employee> lines = new ArrayList<>();

    @Getter
    private final CountDownLatch latch ;

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

        final Matcher matcher = pattern.matcher(s);
        if (!matcher.matches()) {
            throw new RuntimeException("Invalid line: " + s);
        }

        final Employee employee = new Employee() {
            {
                id = matcher.group(1);
                name = matcher.group(2);
                age = Integer.parseInt(matcher.group(3));
                ssn = Integer.parseInt(matcher.group(4));
                email = matcher.group(5);
            }
        };

        lines.add(employee);
        latch.countDown();
    }

    @Override
    public void handle(Exception e) {
        tailer.stop();
    }
}
