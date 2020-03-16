# Schema Registry JMH Microbenchmarks

This module is for JMH micro-benchmarking parts of Schema Registry.

### How to run

The benchmarks can be run either from `SerdeBenchmark.java` directly through IntelliJ, or via the
command line as follows, after building the module to produce `target/benchmarks.jar`:

```
java -jar ./target/benchmarks.jar
```

### Running a subset of benchmarks

To run only a subset of the benchmarks, you can specify parameters to run with. For example,
to run only AVRO benchmarks:
```
java -jar ./target/benchmarks.jar -p serializationFormat=AVRO
```

### Running with non-default parameters

JMH parameters of interest may include the number of forks to use (`-f`), the number of warmup and
measurement iterations (`-wi` and `-i`, respectively), the duration of each iteration
(`-w` and `-r` for warmup and measurement iterations, respectively, with units of seconds),
and the number of threads (`-t`).
By default, `SerdeBenchmark.java` is set up to run with 3 forks, 3 warmup iterations, 3 measurement
iterations, 10 seconds per iteration, and 4 threads.

As an example, to run benchmarks with 8 threads and only a single fork:
```
java -jar ./target/benchmarks.jar -t 8 -f 1
```

The full list of JMH command line options can be viewed with:
```
java -jar ./target/benchmarks.jar -h
```

