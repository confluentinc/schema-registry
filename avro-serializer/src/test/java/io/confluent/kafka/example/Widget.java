package io.confluent.kafka.example;

import java.util.Objects;

public class Widget {
    private String name;

    public Widget() {}
    public Widget(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Widget widget = (Widget) o;
        return name.equals(widget.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
