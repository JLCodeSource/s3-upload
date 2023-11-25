# s3-upload

## Todo

1. Add pre-upload checksum verification
2. Add pre-download checksum verification
3. Add tests for no object
4. Add worker queues for hashing local files
5. Add worker queues for uploading local files
6. Manage status with hashmap 
(Key: filepath, Value: None=ToBeHashed,sha=ToBeUploaded,Done=Done)
7. Dump hashmap to file & manage work after stop