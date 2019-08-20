# VMware-Go-KCL

## Overview

[Amazon Kinesis](https://aws.amazon.com/kinesis/data-streams/)  enables real-time processing of streaming data at massive scale. Kinesis Streams is useful for rapidly moving data off data producers and then continuously processing the data, be it to transform the data before emitting to a data store, run real-time metrics and analytics, or derive more complex data streams for further processing.

The **Kinesis Client Library for Go** (goKCL) enables Go developers to easily consume and process data from [Amazon Kinesis][kinesis]. goKCL has the same API and functional spec of the [Java KCL v2.0](https://docs.aws.amazon.com/streams/latest/dev/kcl-migration.html) without the resource overhead of installing Java based MultiLangDaemon.

## Try it out

### Prerequisites

- Install [Go](https://golang.org/)
- Config [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html)

## Documentation

goKCL matches interface and programming model from original Amazon KCL, the best place for getting reference, tutorial is from Amazon itself:

- [Developing Consumers Using the Kinesis Client Library](https://docs.aws.amazon.com/streams/latest/dev/developing-consumers-with-kcl.html)
- [Troubleshooting](https://docs.aws.amazon.com/streams/latest/dev/troubleshooting-consumers.html)
- [Advanced Topics](https://docs.aws.amazon.com/streams/latest/dev/advanced-consumers.html)

