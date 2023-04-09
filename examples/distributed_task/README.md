# Distributed Task Performance Test

This repository contains a performance testing script for DistributedTask, which is a part of the aiodistributor
package. The test measures the performance of a distributed task system by executing an asynchronous function that
sleeps for 1 second across multiple nodes.

### Code Overview

The main components of the code are:

- ```performance_test_function():``` The asynchronous function that is executed as a distributed task. It records the
  start and end times of the task execution and appends the results to the global EXEC_STATS list.

- ```measure_performance(period: float):``` The main performance testing function that initializes the DistributedTask,
  starts it, sleeps for the specified period, stops the task, and appends the execution statistics to a Redis database.

- ```The main block:``` Sets the testing period from an environment variable (default is 5 seconds) and runs the
  measure_performance function using asyncio.run().

### Usage

1. ##### Start a Redis server

2. ##### Build and run docker containers
   specify number of workers instead of ```N```
   ```commandline
    docker-compose up -d --scale worker=N
   ```

#### After the test is complete, the execution statistics will be printed in the stdout of container and stored in the Redis database under the key stats.
