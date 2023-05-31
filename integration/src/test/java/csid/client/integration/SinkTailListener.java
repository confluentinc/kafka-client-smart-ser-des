package csid.client.integration;

import csid.client.integration.model.Employee;
import lombok.Getter;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SinkTailListener implements TailerListener {

    private final Pattern pattern = Pattern.compile("([A-Za-z]*=[A-Za-z0-9_@.]*)+");

    @Getter
    private final List<Employee> lines = new ArrayList<>();

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

        final Employee employee = new Employee();

        final Matcher matcher = pattern.matcher(s);
        while (matcher.find()) {
            final String value = matcher.group();
            final String[] elements = value.split("=");
            switch (elements[0]) {
                case "ID":
                    employee.ID = elements[1];
                    break;
                case "Name":
                    employee.Name = elements[1];
                    break;
                case "Email":
                    employee.Email = elements[1];
                    break;
                case "Age":
                    employee.Age = Integer.parseInt(elements[1]);
                    break;
                case "SSN":
                    employee.SSN = Integer.parseInt(elements[1]);
                    break;
            }
        }

        if (employee.isValid()) {
            lines.add(employee);
        }

        latch.countDown();
    }

    @Override
    public void handle(Exception e) {
        tailer.stop();
    }
}
