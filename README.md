# broadcast-join
An improvised Spark join which performs broadcast join/map-sode join if one of the 2 RDDs has a size estimate less than configurable broadcast join threshold.
