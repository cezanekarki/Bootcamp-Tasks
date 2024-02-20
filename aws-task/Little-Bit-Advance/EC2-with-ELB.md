# EC2 with Elastic Load Balancer (ELB)

## Elastic Load Balancer

- Elastic Load Balancing (ELB) automatically distributes incoming application traffic across multiple targets and virtual appliances in one or more availability zone (AZ)

    - `Application Load Balancer`
        - ALB operates at the application layer(layer 7) of the OSI model and is best sutied for ditributing HTTP/HTTPS, gRPC,Websockets.
   ![loadbalancer1](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/b837b05b-cbdc-43d6-b5d9-1a924af605b9)
    - `Gateway Load Balancer`
        - GLB is designed fro routing traffic to third-party secrity appliances like firewalls and intrusion detection systems.
    - `Network Load Balancer`
        - NLB operates at the transport layer (layer 4) and is designed for handling TCP,UDP and TLS traffic.



## Application Load Balancer for EC2 instance

- To create a load balancer using the AWS Management Console, complete the following tasks.

 - Step 1: Configure a target group
 - Step 2: Register targets
 - Step 3: Configure a load balancer and a listener
 - Step 4: Test the load balancer

![loadbalancer2](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/1e0b9e05-d0e7-4f01-9279-666f863894f3)


**Link for more detail**: - `https://docs.aws.amazon.com/elasticloadbalancing/latest/application/create-application-load-balancer.html`

- `Target Group`
    - Target groupos route request to individual registered targets, such as EC2 instanaces,using the protocol and port numbers that you specify.
    - Each target group is used to route requests to one or more registered targets.
- **`NOTE`**
    - ** when we create application load balancer we need to have default + another new security group for incomming traffice from http port 80 (allow http request) or any other internet access **


## EC2 with ALB

### Overview 

![applicationloadbalacer-practise](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/d772154f-ca12-4582-9fdd-0ad766bd633f)


- first let's create `VPC`

![ec2-alb-1](https://github.com/anupmaharzn/aws-task/assets/34486226/8c31a94d-895d-44fe-b56a-4940c8fda6f0)


- as we can see our `VPC` has been created.

![ec2-alb-2](https://github.com/anupmaharzn/aws-task/assets/34486226/12edff62-a274-49f9-9f2d-9ee600a99d6f)


- let's create `internet gateway` and attach it to our `VPC`

![ec2-alb-3](https://github.com/anupmaharzn/aws-task/assets/34486226/a1d37e9c-266e-485d-a785-efeafe42bc0c)


- now attach to `VPC`

![ec2-alb-4](https://github.com/anupmaharzn/aws-task/assets/34486226/57c55c0b-c285-4626-a4eb-ea70ba21b69a)


- now let's create `2 public subnets`

![ec2-alb-5](https://github.com/anupmaharzn/aws-task/assets/34486226/6cae3710-af80-4c35-a27c-a239ff32b0fa)



- Create `Route Table` for our vpc so that each subnet in a vpc can associated with a route table,which contorls the traffic between subnets and defines the routes for traffic leaving the VPC.

- In an Amazon VPC, `each subnet` must be associated with `one and only one` `route table` at any given time.


![ec2-alb-6](https://github.com/anupmaharzn/aws-task/assets/34486226/2c2d3386-e78f-487c-8d45-0fb8ce3b7417)



- associate the subnets with this route table

![ec2-alb-7](https://github.com/anupmaharzn/aws-task/assets/34486226/61a90f18-b57a-420e-bf37-cc570d3325f7)


- now adding `internet gateway` to the route table

![ec2-alb-8](https://github.com/anupmaharzn/aws-task/assets/34486226/d1740d84-84d4-45ee-83ac-4fdc4a135be9)


- Let's create `two EC2 instance` ,assocaite `one instance for each subnet`

![ec2-alb-9](https://github.com/anupmaharzn/aws-task/assets/34486226/1c37b106-7087-458e-94ed-66e016210c9f)


    - `similar for another instnace for different subnet`

- `now we have finished provisioning our EC2 instances`


- let create our `target group` and add our targets which is nothing but our ec2 insances

![ec2-alb-10-1](https://github.com/anupmaharzn/aws-task/assets/34486226/41c8954d-8cef-4814-afe7-67f0288b335b)


![ec2-alb-10-2](https://github.com/anupmaharzn/aws-task/assets/34486226/96fa54bb-c689-4f7e-b36b-bebc1ef3e903)


![ec2-alb-10-3](https://github.com/anupmaharzn/aws-task/assets/34486226/7f3f53f0-b3a6-4d44-9826-dcbbd0ba2c98)


![ec2-alb-10-4](https://github.com/anupmaharzn/aws-task/assets/34486226/4c81ae07-6475-4507-aa2e-d2e3be3fbe81)


![ec2-alb-10-5](https://github.com/anupmaharzn/aws-task/assets/34486226/5bc1f771-8f47-448d-999e-7304ed29a4d1)



- Now create `Application Load Balancer`

    - in ALB we have to configure the security group to `allow inbound traffic` from `http protocal port 80` for our `ALB`

   ![ec2-alb-11](https://github.com/anupmaharzn/aws-task/assets/34486226/79a8bf67-c4b2-40c0-9861-00f310f1f441)


- now `Create ALB`

![ec2-alb-12-1](https://github.com/anupmaharzn/aws-task/assets/34486226/8449d9b2-aa7e-408e-a7a1-38f50409b8a0)


![ec2-alb-12-2](https://github.com/anupmaharzn/aws-task/assets/34486226/052cf6fb-88b3-494d-b027-0916ca2817e6)

![ec2-alb-12-3](https://github.com/anupmaharzn/aws-task/assets/34486226/6b821468-1f3f-40da-b333-b0639c76ac2a)


![ec2-alb-12-4](https://github.com/anupmaharzn/aws-task/assets/34486226/2bb23291-0f36-4f47-86d2-98c46f75cbd5)




**we have completed the EC2 with ALB**

- now we can see it's working

![ec2-alb-13](https://github.com/anupmaharzn/aws-task/assets/34486226/d2af73ed-4227-46d6-b3e3-c3b1b9aa1b28)


