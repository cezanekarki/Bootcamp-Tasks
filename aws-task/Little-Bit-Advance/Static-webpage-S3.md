# Hosting a Static Portfolio Website on S3

**Objective**: Learn to host a static website (such as a personal portfolio) on Amazon S3.

**Approach**:

1. **Create an S3 Bucket**: Start by creating a new S3 bucket. Configure the bucket for website hosting, which includes setting permissions to make the content publicly accessible.
2. **Upload Website Files**: Upload the static files of your portfolio website (HTML, CSS, JavaScript, images) to the S3 bucket.
3. **Configure DNS**: Use Amazon Route 53 or another DNS service to point a domain name to the S3 bucket. This makes the website accessible via a user-friendly URL.
4. **Enable Additional Features** (Optional): Implement features like HTTPS for secure access and CloudFront for content delivery optimization.

**Goal**: Students will understand how to use S3 for hosting static websites, manage bucket permissions, and integrate with other AWS services for a complete web hosting solution.


- **Let's Start**

- First , Create a bucket

![stat-1](https://github.com/anupmaharzn/aws-task/assets/34486226/b8ad7aa3-352a-4873-8ad8-f60286c4551b)



- now, add bucket policies

![stat-2](https://github.com/anupmaharzn/aws-task/assets/34486226/a7e57663-0091-4c2f-a478-6f40e47cf38c)



- go to `properties` tab and enable static hosting

![stat-3](https://github.com/anupmaharzn/aws-task/assets/34486226/9596de2a-8e1b-4bcd-a7ab-f9375275b274)



- now let's upload our static file into s3 bucket

![stat-4](https://github.com/anupmaharzn/aws-task/assets/34486226/7b85e11b-09f5-4c06-a358-3d82f22195a5)


- now , if we one this `url` , we can see our hosted website

![stat-5](https://github.com/anupmaharzn/aws-task/assets/34486226/8a058f03-3ef2-41a6-9113-69ff808cd0af)


- **Our static Website**

![stat-6](https://github.com/anupmaharzn/aws-task/assets/34486226/ef49e650-eab5-49fd-bf45-82bfacd0a3c5)



- **Route 53 domain not accessiable in this account**

#img7![stat-7](https://github.com/anupmaharzn/aws-task/assets/34486226/bbec165b-e69c-4caf-a9ef-c07dad85b2a3)
