# EC2 with Auto Scaling Group


## AWS EC2 Auto Scaling
    
- `Auto Scaling Groups`

- Amazon EC2 auto scaling helps you maintain application availability and allows you to `dynamically adjust `the `capacity` of your Amazon EC2 instances based on the demand for your applications.

- commonly used for following secnarios:
    - maintaining application availability
    - cost optimization
    - fault tolerance
    - elasticity: scale seamlessly 

![autoscaling1](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/b7bbf81d-fcb4-4d23-a378-4be7ca3dc3de)


- **For More Detail**
    - `https://docs.aws.amazon.com/autoscaling/ec2/userguide/get-started-with-ec2-auto-scaling.html`


- `NOTE`
    - Instances of EC2 are typically created and managed using `Auto Scaling Policies`
    - The Purpose of Amazon EC2 auto scaling is to autmatically adjust the number of instances in an `auto scaling group `based on `changes in demand `or other specified condition.
- `While create the instance from autoscaling group we need **Lauch Template**`
- `Amazon EC2 Launch Templates` provide a way to specify the configuration of an EC2 instance in a resuable format.
    - A lauch template includes settings such as the
        - `AMI`
        - `instance type`
        - `key pair`
        - `security groups`
        - `other launch configuration parameters`

- `we just have to create one lauch template and auto scaling will automatically scale that instance into multiple instance`

## Lab

### overview architecture

![autoscaling2-practise](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/a4315695-aa5f-4456-b7db-115ce5b5b22c)


**Let's Start**

- Lets first Create our `VPC`
    - as we can see our `test-asg-vp` VPC `is created`

 ![asg-1](https://github.com/anupmaharzn/aws-task/assets/34486226/08307b50-917c-4aec-9a1a-e7c8e92496b3)


- Now let's create our `internet gateway`
  
    - as we can see our `test-asg-igw` internet gateway is created.

 ![asg-2](https://github.com/anupmaharzn/aws-task/assets/34486226/3bce12e8-0223-41bc-b6f8-bc100b662c32)

    - now attach it to vpc.

 ![asg-3](https://github.com/anupmaharzn/aws-task/assets/34486226/6d89e05a-7f8c-4c39-99b1-22ab3131cb43)


- `note in overview we have mentioned only one subnet` but we are creating two subnets

![asg-4](https://github.com/anupmaharzn/aws-task/assets/34486226/c7279658-9c3c-4f2d-be82-9136ddf9a221)


![asg-5](https://github.com/anupmaharzn/aws-task/assets/34486226/f91ad06c-e453-486b-8ab1-2b4248d3abee)


- now create `route table` add `internet gateway` in route and also associate the subnets that we created.
  
- added `igw`
  
![asg-6](https://github.com/anupmaharzn/aws-task/assets/34486226/f1e81bab-11fe-41b3-a56c-7d54a00ae25c)

- associated `subnets`
  
![asg-7](https://github.com/anupmaharzn/aws-task/assets/34486226/e2cb8b65-636c-4421-9892-f6388ff9da5a)

- now lets create `target group`,responsible for pointing to `EC2 instances`

![asg-8](https://github.com/anupmaharzn/aws-task/assets/34486226/5f4465e7-cc9c-4ea9-9e2f-e5cb0553ada1)


- as we can see, we are just creating blank `tg`.
  
![asg-9](https://github.com/anupmaharzn/aws-task/assets/34486226/c5e328fd-3d66-4269-80e0-43bb85dce8c6)



- now create `Application Load Balancer`

    - we also have to create security group from inbound http traffic
      
   ![asg-10](https://github.com/anupmaharzn/aws-task/assets/34486226/3c1f9ce9-78b3-4ddb-a617-97968a0bf0ca)


- now,create `ALB`
  
![asg-11](https://github.com/anupmaharzn/aws-task/assets/34486226/58ca9f90-6bb9-46a7-a93f-f405d46b5f8a)


- after that, it's time to create `Auto Scaling Group`

![asg-12](https://github.com/anupmaharzn/aws-task/assets/34486226/ead6a9d9-208e-4828-8744-a4d98f184d12)


- lets create `Lauch template`

![asg-13](https://github.com/anupmaharzn/aws-task/assets/34486226/3bd11db0-7088-408c-8afe-664d5cb8b9c2)


- now assign this lauch template and create auto scaling group

![asg-14](https://github.com/anupmaharzn/aws-task/assets/34486226/0db11af4-4909-40ee-adfb-88f12ae9b1e6)


![asg-15](https://github.com/anupmaharzn/aws-task/assets/34486226/9f9c1e71-250b-4bb6-8732-d29a89fdd808)


![asg-16](https://github.com/anupmaharzn/aws-task/assets/34486226/c9cf95bc-716f-4840-9872-64580fc3df54)


- now click on create auto scaling group.

![asg-17](https://github.com/anupmaharzn/aws-task/assets/34486226/316018b0-93f9-4628-bbdc-eeccdcf24b5b)



- **once you create auto scaling group it automatically start creating EC2 instances.**
