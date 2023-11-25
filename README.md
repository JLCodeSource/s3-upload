# s3-upload

## Todo

1. Add pre-upload checksum verification
2. Add post-upload status check
3. Add pre-download checksum verification
4. Add tests for no object
5. Add worker queues for hashing local files
6. Add worker queues for uploading local files
7. Manage status with hashmap 
(Key: filepath, Value: None=ToBeHashed,sha=ToBeUploaded,Done=Done)
8. Dump hashmap to file & manage work after stop
9. Improve UI