package io.confluent.kafka.example;

import javax.annotation.Nullable;
import java.util.Objects;

public class NullableWidget {
  private String name;

  @Nullable
  private Integer age;

  public NullableWidget() {}

  public NullableWidget(String name) {
    this.name = name;
  }

  public NullableWidget(String name, Integer age) {
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
    NullableWidget that = (NullableWidget) o;
    return name.equals(that.name) &&
        Objects.equals(age, that.age);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, age);
  }
}
