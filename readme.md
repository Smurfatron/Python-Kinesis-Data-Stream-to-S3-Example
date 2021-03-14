# Python Kinesis Data Stream to S3 Example
This example is based in part on A Cloud Guru's [Streaming Data Collection demo](https://github.com/ACloudGuru-Resources/Course_AWS_Certified_Machine_Learning/tree/master/Chapter3).

It gets randomly generated user data from the [Random User API](https://randomuser.me/) and then puts each record in a Kinesis Data Stream.
From that point, there are two pathways demonstrated:
1. Use a Kinesis Data Firehose delivery stream then processes the Data Stream, transform the records using a Lambda (see code in [lambda_function.py](lambda_function.py)), and put them in an S3 bucket
2. Use a Kinesis Data Analytics application to execute a SQL query against the streaming data, then use a Kinesis Data Firehose delivery stream to put the queried subset in an S3 bucket

### NOTES:
1. Using AWS services for this example **will incur costs**, so be sure to delete any the Kinesis Data Streams, Data Firehose or Data Analytics services you spin up when you're done
2. The code in this repo is not necessarily optimized and may not use the best approach but should provide a quick and simple working example

See the [wiki](https://github.com/Smurfatron/Python-Kinesis-Data-Stream-to-S3-Example/wiki) for details on setting up the services in the AWS Console

The SQL query used in this example extracts a subset of the streaming data where the user's age is > 18:
   <pre>
CREATE OR REPLACE STREAM "DESTINATION_USER_DATA" (
    first VARCHAR(16), 
    last VARCHAR(16), 
    age INTEGER, 
    gender VARCHAR(16), 
    latitude FLOAT, 
    longitude FLOAT
);
CREATE OR REPLACE PUMP "STREAM_PUMP" AS INSERT INTO "DESTINATION_USER_DATA"

SELECT STREAM "first", "last", "age", "gender", "latitude", "longitude"
FROM "SOURCE_SQL_STREAM_001"
WHERE "age" >= 18;
</pre>
