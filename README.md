# [aiodistributor](aiodistributor)

## Python Asynchronous Library for Synchronization of Replicated Microservices

[![codecov](https://codecov.io/gh/malik89303/aiodistributor/branch/dev/graph/badge.svg?token=1QNFR6LTT0)](https://codecov.io/gh/malik89303/aiodistributor)
![tests](https://github.com/malik89303/aiodistributor/actions/workflows/test-branch.yml/badge.svg?branch=dev)

#### This library provides a set of tools for synchronizing replicated microservices using Redis. The main goal is to facilitate inter-service communication, rate limiting, and throttling mechanisms in a distributed environment.

### Features:

- Distributed sliding counter for implementing rate limiting or throttle mechanisms.
- Distributed waiter for waiting for signals from other nodes or services and triggering the appropriate callback.
- Distributed task for managing and distributing tasks across multiple nodes or services.
- Distributed cache for caching data and sharing it among services.
- Distributed notifier for event-driven communication between nodes or services.
- Utilizes Redis for storage and message passing between nodes.
- Asynchronous and non-blocking design using Python's asyncio library.

### Dependencies

- Python 3.10+
- Redis server
- redis-py or aioredis

### Installation

To install the aiodistributor library, you can simply use pip:

```commandline
pip install aiodistributor
```

### Usage

Detailed examples and usage instructions can be found in [examples](examples) folder

### Contributing

Contributions are welcome! Please submit a pull request or create an issue to discuss proposed changes.
