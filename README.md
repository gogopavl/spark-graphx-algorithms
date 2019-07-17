# Spark GraphX Algorithms
Repository containing various graph algorithm implementations using Spark's GraphX API

## Installation

All jobs were run on [Spark 2.4.3](https://spark.apache.org/releases/spark-release-2-4-3.html).

### Prerequisites

1. Java 8+

2. Scala 2.12.x (though uses 2.11.x)

## Running

To submit a job using one of the scripts run the following command:

```
./Spark-2.4.3/bin/spark-shell -i path/to/script --conf spark.driver.args="path/to/dataset"
```

Other useful parameters that can be specified (appended to the command right above):

- `--master local[N]` (where N the number of cores used - e.g. 4, * for all)
- `--driver-memory Ng` (where N the number of GBs - e.g. 4)

If Spark is not used in local mode you can also define:

- `--num-executors N` (where N the number of executors - e.g. 5)
- `--executor-cores N` (where N the number of cores per executor - e.g. 4)
- `--executor-memory Ng` (where N the number of GBs of memory per executor - e.g. 2)

## License

As stated in [LICENSE](https://github.com/pgogousis/spark-graphx-algorithms/blob/master/LICENSE).