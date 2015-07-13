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

##Reference

###Amazon DynamoDB
* [Amazon DynamoDB][dynamodb]
* [AWS Geo Library for Amazon DynamoDB] [dynamodb-query]

[dynamodb]: http://aws.amazon.com/dynamodb
[geo-library-javadoc]: http://awslabs.github.io/dynamodb-geo/
[dynamodb-query]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html