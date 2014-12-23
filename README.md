s3_uploader
===========

Streaming concurrent multipart S3 uploads from stdin. 

Intended for use with sources that stream data fairly slowly (like RDS dumps),
such that getting the initial data is the dominant bottleneck.

Traditionally (eg, using s3cmd) you would have to wait to get all the data (possibly compressing it), writing to a local temporary file, then
upload this file to S3--either monolithically or multipart[1].

With s3_uploader, you pipe your data in via stdin (compressing/encrypting earlier in the pipeline if necessary) and s3_uploader streams to temporary files
and when chunk_size is reached, a goroutine is spawned that uploads that chunk to S3. By tuning the chunk size[2] according to the stream rate and your upload speed you can essentially stream the data to S3 as it comes in, without having to store the entire thing as a temporary file first. Data sources of any size can be used (only tiny amounts of data are buffered in RAM) as long as you have a reasonable amount of temporary disk space. In most cases you do *not* need
to have enough space to store the entire data set on disk at once since individual chunks are deleted after being uploaded to S3 (however if your uploads are really slow it's possible for many chunks acculumlate on disk).

1. Note that s3cmd on Linux only does sequential multipart uploads, as of the current version (1.5.x).
2. S3 has a maximum multipart count of 10000, therefore: total_input_size / chunk_size <= 10000

```
Usage of ./s3_uploader:
  -acl="bucket-owner-full-control": ACL for new object
  -bucket="": S3 bucket name (required)
  -chunk_size=50000000: multipart upload chunk size (bytes)
  -expected_size=0: expected input size (fail if out of bounds)
  -key="": S3 key name (required; use / notation for folders)
  -mime_type="binary/octet-stream": Content-type (MIME type)
  -region="us-west-2": AWS S3 region
  -sse=false: use server side encryption
```

Usage Example:
--------------
```
$ AWS_ACCESS_KEY="foobar"
$ AWS_SECRET_KEY="s3cr3t"
$ mysqldump --host $DB_HOST --user=$DB_USER --password=$DB_PASSWORD --opt --single-transaction --all-databases |gzip |s3_uploader -region "${S3_REGION}" -bucket "${S3_BUCKET}" -key "${S3_KEY}" -sse "${S3_SSE}"
```
