# [ParStream Storm Bolt](http://www.parstream.com)

ParStream is a massively parallel (MPP) data management system designed to run complex analytical queries over extremely large amounts of data on a cluster of commodity servers.

Apache Storm is a free open source distributed realtime computation system.

This ParStream bolt implementation is an Apache Storm bolt consuming a stream of tuples. Each tuple is converted into the corresponding ParStream format and then streamed into the ParStream database.
