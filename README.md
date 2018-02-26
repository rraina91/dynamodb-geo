[![Build Status](https://travis-ci.org/CorkHounds/dynamodb-geo.svg?branch=master)](https://travis-ci.org/CorkHounds/dynamodb-geo)

#Geo Library for Amazon DynamoDB

This library was forked from the [AWS geo library][geo-library-javadoc].

Following limitations with the aws geo library were the main reasons that necessitated this fork:
* Usage required a table’s hash and range key to be replaced by geo data. This approach is not feasible as it cannot be used when performing user-centric queries, where the hash and range key have to be domain model specific attributes.
* Developed prior to GSI, hence only used LSI
* No solution for composite queries. For e.g. “Find something within X miles of lat/lng AND category=‘restaurants’;
* The solution executed the queries and returned the final result. It did not provide the client with any control over the query execution.

## What methods are available for geo-querying in this library?
* Query for a given lat/long
* Radius query
* Box/Rectangle query

All of the above queries can be run as composite queries, depending on their geoConfig.
Result of a geo query is a _GeoQueryRequest_ object. A GeoQueryRequest object is a wrapper around a list of dynamo’s QueryRequest objects and a GeoFilter that should be applied to the queries.

Benefit of this approach - Callers have the option to execute these queries in a way they desire (multi-threaded, map-reduce jobs, etc).

## Creating an item with geo data
* Caller passes in their existing _PutItemRequest_
* The request gets decorated with the “geo-code” related information and is returned to the client
* YOU control how you want to save your data!
  * Bulk persistence
  * Large Item persistence strategy (limit on item size in dynamo)

##Features
* **Box Queries:** Get a list of _GeoQueryRequest_ objects that will return items that fall within a pair of geo points that define a rectangle as projected onto a sphere.
* **Radius Queries:** Get a list of _GeoQueryRequest_ objects that will return all of the items that are within a given radius of a geo point.
* **Composite Queries:** Get a list of _GeoQueryRequest_ objects that will return all of the items that are within a given radius and has a property 'X'
* **Easy Integration:** The library simply _decorates_ the provided _PutItemRequest_ and _QueryRequest_ with geo-data so you get to control the execution of queries. (multi-threaded, map-reduce jobs, etc)
* **Customizable:** Geo column names and related configuration can be set in the _GeoConfig_ object

##Getting Started
###Setup Environment
1. **Sign up for AWS** - Before you begin, you need an AWS account. Please see the [AWS Account and Credentials][docs-signup] section of the developer guide for information about how to create an AWS account and retrieve your AWS credentials.
2. **Download Geo Library for Amazon DynamoDB** - To download the code from GitHub, simply clone the repository by typing: `git clone https://github.com/Dash-Labs/dynamodb-geo.git`.

##Building From Source
Once you check out the code from GitHub, you can build it using [Ply](https://github.com/blangel/ply.git): `ply clean install`

##Limitations

###High I/O needs
Geo query methods will return several queries. Depending on your configuration, this could be thousands of queries

###Dataset density limitation
The Geohash used in this library is roughly centimeter precision. Therefore, the library is not suitable if your dataset has much higher density.

## Choosing geohash length based on km/m
* http://www.metablake.com/foursquare/wsdm2013-final.pdf - shows S2 lib has 31 levels, giving <1cm^2 at level 31
* http://karussell.wordpress.com/2012/05/23/spatial-keys-memory-efficient-geohashes/ - shows the math to compute for your sphere diameter - diameter / 2^number_levels
* http://unterbahn.com/2009/11/metric-dimensions-of-geohash-partitions-at-the-equator/ - describes how this varies depending upon latitude
* http://ravendb.net/docs/2.0/client-api/querying/static-indexes/spatial-search - gives a great breakdown from 1 - 30
* http://www.easysurf.cc/circle.htm#cetol1 - calculate earth circumference at given latitude

So, given above, here are some common geohash levels (base 0) mapped to distances:

Level | dyanmodb-geohash length | Distance
--- | --- | ---
19 | 12-13 | ~38m at equator (or **~15m** at nyc latitude)
15 | 10 | ~611m at equator (or **~230m** at nyc latitude)
9 | 6 |~39km at equator (or **~15km** at nyc latitude)
6 | 4 | ~313km at equator (or **~118km** at nyc latitude)

## Geo querying analysis

#### New strategy - 10 threads, hashkey length = 5, 20m query followed by 50 m.
Lat/long|Time taken(s) in Dynamo| Time taken(s) in Mongo 
---|---|---|
40.727526, -73.9944511|0.174|0.054
38.114560, -117.270763|0.258|0.053

### HashKey length - 6:

Radius(miles) | Number of queries fired 
---|---|
600|30-->23281
500|9-->19136
50|53-->155
40|31-->105
30|10-->66
20|1-->57
10|6-->10
1m|1-->1

### HashKey length - 5:

Radius(miles) | Number of queries fired | Time taken(s) | Time taken(s) w/10 threads 
---|---|---|---|
600|30-->2356
500|9-->1921
50|53-->63|1.89|0.37
40|31-->39|1.134|0.32
30|10-->16|0.67|0.25
20|1-->7|0.43|0.19
10|6-->6|0.27|0.13
1m|1-->1

### HashKey length - 4:

Radius(miles) | Number of queries fired 
---|---|
600|30-->262
500|9-->200
50|53-->55
40|31-->33
30|10-->11
20|1-->2
10|6-->6
1m|1-->1

### HashKey length - 3:

Radius(miles) | Number of queries fired 
---|---|
600|30-->53
500|9-->26
50|53-->54
40|31-->32
30|10-->11
20|1-->2
10|6-->6
1m|1-->1

### HashKey length - 2:

Radius(miles) | Number of queries fired 
---|---|
600|30-->32
500|9-->10
50|53-->53
40|31-->31
30|10-->10
20|1-->1
10|6-->6
1m|1-->1

### HashKey length - 1:

Radius(miles) | Number of queries fired 
---|---|
500|9-->9
50|53-->53
40|31-->31
30|10-->10
20|1-->1
10|6-->6
1m|1-->1

Number of tries to find zip codes (base radius of 10 miles) with hashKey length of 5:
* NYC 40.727526, -73.9944511 : 1
* Haskell County(Texas) 33.215714, -99.814544 : 1
* Torrence County (New Mexico) 34.479959, -105.641418 : 2
* Augusta (Arkansas) 35.292151, -91.260314 : 1
* Tonopah (Nevada)** 38.114560, -117.270763 : 4
* Gypsum(CO)** 39.814929, -106.393789 : 1

** For Tonopah, all levels generated 26 queries!
** For Gypsum(CO)**, 10 miles generated 16 queries, 20 miles generated 30, 30 miles generated 45.

##Reference

###Amazon DynamoDB
* [Amazon DynamoDB][dynamodb]
* [AWS Geo Library for Amazon DynamoDB] [dynamodb-query]

[dynamodb]: http://aws.amazon.com/dynamodb
[geo-library-javadoc]: http://awslabs.github.io/dynamodb-geo/
[dynamodb-query]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html
