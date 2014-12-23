s3_uploader
===========

Streaming concurrent multipart S3 uploads from stdin. 

Intended for use with sources that stream data fairly slowly (like RDS dumps),
such that getting the initial data is the dominant bottleneck.

Traditionally (eg, using s3cmd) you would have to get all the data (possibly compressing it), writing to a local temporary file, then
upload this file to S3--either monolithically or multipart[1].

With s3_uploader, you pipe your data in via stdin (compressing/encrypting earlier in the pipeline if necessary) and s3_uploader streams to temporary files
and when chunk_size is reached, a goroutine is spawned that uploads that chunk to S3. By setting the chunk size somewhat aggressively[2] you can essentially
stream the data to S3 as it comes in, without having to store the entire thing as a temporary file first. Data sources of any size can be used (only tiny amounts of data are buffered in RAM) as long as you have a reasonable amount of temporary disk space. In most cases you do *not* need
to have enough space to store the entire data set on disk at once since individual chunks are deleted as uploaded to S3 (unless your uploads are really slow and the chunks acculumlate on disk).

1. Note that s3cmd on Linux only does single-threaded uploads, even when multipart uploading is enabled.
2. S3 has a maximum multipart count of 10000, therefore: total_input_size / chunk_size <= 10000

Usage Example:
--------------

