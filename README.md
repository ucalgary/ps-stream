# Process PeopleSoft Sync Messages into Kafka Topics

[![](https://images.microbadger.com/badges/image/ucalgary/ps-sync-kafka.svg)](https://microbadger.com/images/ucalgary/ps-sync-kafka)

`pssync` is a Python utility that collects and and parses PeopleSoft sync and fullsync messages into Kafka topics. PeopleSoft sync processes are normally used to sync data between PeopleSoft applications. However, they can also be used as a way to generate an externalized stream of PeopleSoft objects in streaming data pipelines.

There are two major commands in `pssync`.

**`collect`** accepts sync messages from PeopleSoft applications over http or https and stores them in Kafka topics by message name.

**`parse`** consumes sync messages from one or more Kafka topics, and generates new Kafka topics with `KTable` semantics. Each record in the resulting stream is oriented to reflect records whose record key is the primary key or identifier of the record.

## Running a pssync container

Collect PeopleSoft sync messages.

```
$ docker run -p 8000:8000 -d ucalgary/ps-sync-kafka collect
```

## Maintenance

This repository and image are currently maintained by the Research Management Systems project at the [University of Calgary](http://www.ucalgary.ca/).
