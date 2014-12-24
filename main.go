// Concurrent, mulipart S3 upload optimized for RDS dumps

package main

import (
	"flag"
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"github.com/crowdmob/goamz/s3"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

const (
	TMP_BUFFER_SIZE   = 10000
	DEFAULT_REGION    = "us-west-2"
	DEFAULT_MIME_TYPE = "binary/octet-stream"
)

var s3_bucket = flag.String("bucket", "", "S3 bucket name (required)")
var s3_key = flag.String("key", "", "S3 key name (required; use / notation for folders)")
var s3_region = flag.String("region", DEFAULT_REGION, "AWS S3 region")
var chunk_size = flag.Int64("chunk_size", 50000000, "multipart upload chunk size (bytes)")
var mime_type = flag.String("mime_type", DEFAULT_MIME_TYPE, "Content-type (MIME type)")
var expected_size = flag.Int64("expected_size", 0, "expected input size (fail if out of bounds)")
var acl_string = flag.String("acl", "bucket-owner-full-control", "ACL for new object")
var use_sse = flag.Bool("sse", false, "use server side encryption")
var retrys = flag.Uint("retrys", 4, "number of retry attempts per chunk upload")
var multi_error struct {
	sync.Mutex
	error bool
}

var aws_auth, aws_auth_err = aws.EnvAuth()

func init() {
	flag.Parse()
	if *s3_bucket == "" {
		fmt.Fprintf(os.Stderr, "S3 bucket parameter missing\n")
		os.Exit(1)
	}
	if *s3_key == "" {
		fmt.Fprintf(os.Stderr, "S3 key parameter missing\n")
		os.Exit(1)
	}
	if *chunk_size < 5000000 || *chunk_size > 5000000000 { // 5MB <= x <= 5GB
		fmt.Fprintf(os.Stderr, "Invalid chunk size: must be between 5MB and 5GB (inclusive)\n")
		os.Exit(1)
	}
	if os.Getenv("AWS_ACCESS_KEY") == "" || os.Getenv("AWS_SECRET_KEY") == "" {
		fmt.Fprintf(os.Stderr, "AWS credentials must be passed as environment variables\n")
		os.Exit(1)
	}
	if aws_auth_err != nil {
		fmt.Fprintf(os.Stderr, "AWS authentication error\n")
		os.Exit(1)
	}
	multi_error.Lock()
	multi_error.error = false
	multi_error.Unlock()
}

func main() {
	s := s3.New(aws_auth, aws.Regions[*s3_region])
	options := s3.Options{
		SSE: *use_sse,
	}
	b := s.Bucket(*s3_bucket)
	m, err := b.Multi(*s3_key, *mime_type, s3.ACL(*acl_string), options)
	if err != nil {
		log.Fatalf("Error initializing multipart upload: %v\n", err)
	}

	log.Printf("Starting multipart upload\n")
	log.Printf("Region: %v\n", *s3_region)
	log.Printf("Bucket: %v\n", *s3_bucket)
	log.Printf("Key: %v\n", *s3_key)
	log.Printf("Chunk size: %v\n", *chunk_size)

	read_buffer := make([]byte, TMP_BUFFER_SIZE)
	current_file_size := int64(0)
	current_chunk_index := 0
	temp_files := make([]*os.File, 1)
	temp_files[0] = new_temp_file(current_chunk_index)
	part_chans := make([]chan s3.Part, 0)
	var uploads sync.WaitGroup

	expected_count := int64(0)
	expected_read_count := int64(0)
	if *expected_size != 0 {
		expected_count = *expected_size / *chunk_size
		if *expected_size%*chunk_size > 0 {
			expected_count += 1
		}

		expected_read_count = *expected_size / int64(TMP_BUFFER_SIZE)
		if *expected_size%int64(TMP_BUFFER_SIZE) > 0 {
			expected_read_count += 1
		}
	}

	i := int64(0)
	n, stdin_err := os.Stdin.Read(read_buffer)
	for stdin_err == nil {
		i += 1
		abort_if_error(m)
		if expected_read_count > 0 && i > expected_read_count {
			log.Fatalf("read count overflow!\n")
		}
		if n > 0 {
			var w_err error
			if n == TMP_BUFFER_SIZE {
				_, w_err = temp_files[current_chunk_index].Write(read_buffer)
			} else {
				write_buffer := make([]byte, n)
				copy(write_buffer, read_buffer)
				_, w_err = temp_files[current_chunk_index].Write(write_buffer)
			}
			if w_err != nil {
				log.Fatalf("Error writing to temp file: %v: %v\n", temp_files[current_chunk_index].Name(), err)
			}
			temp_files[current_chunk_index].Sync()
			current_file_size += int64(n)
			stat, _ := temp_files[current_chunk_index].Stat()
			if current_file_size != stat.Size() {
				log.Fatalf("Temp file size (%v) does not equal expected size (%v): %v\n",
					stat.Size(), current_file_size, temp_files[current_chunk_index].Name())
			}
			if current_file_size >= *chunk_size {
				upload_temp_file(temp_files[current_chunk_index], uploads, current_chunk_index, m, &part_chans)
				current_chunk_index += 1
				current_file_size = int64(0)
				temp_files = append(temp_files, new_temp_file(current_chunk_index))
			}
		}
		n, stdin_err = os.Stdin.Read(read_buffer)
	}
	if current_file_size > 0 {
		upload_temp_file(temp_files[current_chunk_index], uploads, current_chunk_index, m, &part_chans)
	}
	defer cleanup(temp_files)

	uploads.Wait()
	abort_if_error(m)

	parts := make([]s3.Part, len(part_chans))
	for i := range part_chans {
		parts[i] = <-part_chans[i]
	}

	log.Printf("Total chunks: %v\n", len(parts))
	log.Printf("Finalizing multipart upload\n")

	err = m.Complete(parts)
	if err != nil {
		log.Fatalf("Error finalizing upload: %v\n", err)
	}

	log.Printf("Mulipart upload complete\n")
}

func abort_if_error(m *s3.Multi) {
	multi_error.Lock()
	if !multi_error.error {
		multi_error.Unlock()
	} else {
		multi_error.Unlock()
		m.Abort()
		log.Fatalf("Multipart upload aborted due to error(s)\n")
	}
}

func upload_temp_file(f *os.File, uploads sync.WaitGroup, ci int, m *s3.Multi, pc *[]chan s3.Part) {
	f.Close()
	uploads.Add(1)
	c := make(chan s3.Part, 1)
	go s3_part_upload(ci, f, m, c, uploads)
	*pc = append(*pc, c)
}

func cleanup(tf []*os.File) {
	// this should never find temporary files since goroutines should have deleted them
	for i := range tf {
		tf[i].Close()
		if _, err := os.Stat(tf[i].Name()); err == nil {
			log.Printf("Warning: temporary file found (cleaning up): %v (chunk %v)\n", tf[i].Name(), i)
			os.Remove(tf[i].Name())
		}
	}
}

func new_temp_file(ci int) *os.File {
	f, err := ioutil.TempFile("", fmt.Sprintf("s3upload-chunk-%v", ci))
	if err != nil {
		log.Fatalf("Chunk %v: error creating tempfile: %v\n", ci, err)
	}
	log.Printf("chunk %v: temp file: %v\n", ci, f.Name())
	return f
}

func s3_part_upload(ci int, i *os.File, m *s3.Multi, c chan s3.Part, uploads sync.WaitGroup) {
	defer uploads.Done()

	tn := i.Name()

	// re-open input file for reading
	f, err := os.Open(tn)
	if err != nil {
		log.Printf("Chunk %v: error opening input file: %v: %v\n", ci, tn, err)
		multi_error.Lock()
		multi_error.error = true
		multi_error.Unlock()
		return
	}
	defer f.Close() //defer in case of error even though we explicitly close below

	stat, err := f.Stat()
	if err != nil {
		log.Printf("Chunk %v: error getting input file info: %v: %v\n", ci, tn, err)
		multi_error.Lock()
		multi_error.error = true
		multi_error.Unlock()
		return
	}

	var p s3.Part
	var u_err error
	for i := uint(1); true; i++ {
		log.Printf("Chunk %v: starting upload (%v; size: %v)\n", ci, tn, stat.Size())
		p, u_err = m.PutPart(ci+1, f)
		if u_err != nil {
			log.Printf("Chunk %v: upload error: %v (%v)\n", ci, u_err, tn)
			if i <= *retrys {
				log.Printf("Chunk %v: retrying (%v/%v)", ci, i, *retrys)
			} else {
				multi_error.Lock()
				multi_error.error = true
				multi_error.Unlock()
				return
			}
		} else {
			c <- p
			break
		}
	}

	log.Printf("Chunk %v: upload success (N: %v, ETag: %v, Size: %v)\n", ci, p.N, p.ETag, p.Size)

	// explicitly close prior to deleting file
	i.Close()
	f.Close()
	err = os.Remove(tn)
	if err != nil {
		log.Printf("Chunk %v: error deleting temp file: %v: %v\n", ci, tn, err)
	}
}
