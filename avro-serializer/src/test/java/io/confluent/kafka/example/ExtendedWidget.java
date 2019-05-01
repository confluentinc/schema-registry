package io.confluent.kafka.example;

import java.util.Objects;

public class ExtendedWidget {
    private String name;
    private Integer age;

    public ExtendedWidget() {}

    public ExtendedWidget(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExtendedWidget that = (ExtendedWidget) o;
        return name.equals(that.name) &&
                age.equals(that.age);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, age);
    }
}
