# S3 Storage

- **Objective**: To gain hands-on experience with Amazon S3 by performing basic storage operations.
- **Approach**: This lab involves creating an S3 bucket, uploading files to it, and setting up bucket policies for access control. Students will explore the S3 management console, learn about object storage, and understand the concepts of buckets and objects.
- **Goal**: Students will understand how to use S3 for storing and managing data, learn about S3 security and permissions, and become familiar with S3's user interface.



## Amazon S3 (Simple Storage Service)

- Amazon S3 is `highly scalable` and widely used `object storeage service.`

- What objects in S3 can be:
    - `Files`
        - text,images,videos,audio files,PDFs,Words,SpreadSheets etc
    - `Databases`
        - Backups or snapshots of databases can be stored as objects in S3
    - `Logs`
    - `Static Website Content`
        - S3 is commonly used to host static website content.
    - `Containers and Images`
        - Container images for services like Docker can be stored in S3.

- S3 is designed to store and retrieve any amount of data from anywhere on the web.

- S3 is an object storage service,meaning it stores data as objects.
    - Each object consists of data, a unique key and metadata.

- S3 stores data as objects in resources that are called `Bucket`.
    - `Bucket` is a fundamental container for storing and organizing objects.
    - It is a top-level container with a `globally unique name ` within the S3 service.

![storage4](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/3ac70579-6a7b-4b94-a718-ad7153604adf)


- `path-style` url endpoint is normally used when you need to access objects
- `virtual-hosted-style` url endpoint is used when you are using `bucket as a website` for static data.


- Access the data anywhere thru
    - `aws management console`
    - `aws command line interface`
    - `sdk`



## Lab Task

- Create a s3 bucket
  
![s3-1](https://github.com/anupmaharzn/aws-task/assets/34486226/a80b3f02-dfa0-423a-bab2-d7e7dac5d218)


- Bucket created
  
![s3-2](https://github.com/anupmaharzn/aws-task/assets/34486226/99a58ca3-c06a-47cd-a660-b0d14e912111)


- upload a file

![s3-3](https://github.com/anupmaharzn/aws-task/assets/34486226/11dcf675-9c7c-4322-b584-cfdf225a7cdc)



- **changing permission for public access of bucket objects**

    - first change the **`block all public access`** `off`
  
    ![s3-4](https://github.com/anupmaharzn/aws-task/assets/34486226/91f05db6-7aa4-4100-a1f2-ce0bf1af5872)

    - then add `bucket policy` so that we can `read` the bucket object `publically`
  
    ![s3-5](https://github.com/anupmaharzn/aws-task/assets/34486226/f7c92998-5b12-46cd-8aa0-497ffe76fd5b)


- now we can see that object is accessiable thru `object URL`
  
![s3-6](https://github.com/anupmaharzn/aws-task/assets/34486226/c0f79acf-550b-4bd4-8b7d-1d63c0c094b1)



