# VPC 

## Amazon VPC

- Enables you to provision a `logically isolated` section of the AWS Cloud where you can launch AWS resoruces in a virtual network that you define.
- Gives you `control over your virtual networking ` resources including:
    - selection of ip address range
    - creation of subnets
    - configuration of route tables and network gateways
- Enables you to `customize the network configuration` for you VPC
- enables you to use `multiple layer of security`
- 
![vpc1](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/99300dbd-4be9-4e48-a342-7f74aede914f)


## Lab Task Overview

![vpc1](https://github.com/anupmaharzn/aws-task/assets/34486226/1229d99f-4143-4cdb-a181-3a179e7b80c7)


- Create VPC

![vpc2](https://github.com/anupmaharzn/aws-task/assets/34486226/f23f7d99-ff8d-46e4-984e-efca305ad469)


- As we can see VPC is Created

![vpc3](https://github.com/anupmaharzn/aws-task/assets/34486226/f5866679-0df4-4c13-9425-aabdf65a3caa)


- Creating Public and Private Subnet

![vpc4](https://github.com/anupmaharzn/aws-task/assets/34486226/864b6b40-9e4e-46d3-bc4e-8c1f0729fd04)


- Here both of our subnet is ready

![vpc5](https://github.com/anupmaharzn/aws-task/assets/34486226/2bbefe71-3f96-427a-b322-15bc13f6d556)


- Create Internet Gateway

![vpc6](https://github.com/anupmaharzn/aws-task/assets/34486226/71a8a262-fbb4-4cf0-bfb6-ac537b0ef730)


- Now attach it to our vpc

![vpc7](https://github.com/anupmaharzn/aws-task/assets/34486226/1f612f2f-0628-45b0-964c-0221b08ecab8)



- Creating `Route table` for `public subnet`

![vpc8](https://github.com/anupmaharzn/aws-task/assets/34486226/1ebcb4d9-5e23-4a35-8aff-0afd4b94c630)


- after creating public route table attach the `internet gateway in it`

![vpc9](https://github.com/anupmaharzn/aws-task/assets/34486226/a7bc64a7-4010-496e-8445-c2d487b4dcfb)


- now associate `route table` to `public subnet` 

![vpc10](https://github.com/anupmaharzn/aws-task/assets/34486226/87b3bbfb-7856-4c68-a66a-f398ad02a5fd)



- creating `Route table` for `private subnet`

![vpc11](https://github.com/anupmaharzn/aws-task/assets/34486226/8de0a232-0628-4d40-a655-e2a1616aff99)


- after creating `route table` associate to `private subnet` 

![vpc12](https://github.com/anupmaharzn/aws-task/assets/34486226/efeb3e01-6840-442f-8c5e-caf2d341ee07)



- now creating `NAT Gateway` assocaiated with our `public subnet` because public subnet has the `internet gateway`.

![vpc13](https://github.com/anupmaharzn/aws-task/assets/34486226/1f03c0c8-bcb6-4fc7-b9f4-ab521dabb144)



- now update the route table of `private subnet` and add the `nat gateway` to private route table

![vpc14](https://github.com/anupmaharzn/aws-task/assets/34486226/16d049c7-384d-4617-8d07-e904a3d1ecd4)


- **so in this lab we have**
    - created the `vpc`
    - configure the `subnets`(public & private)
    - created the `route table` (public & private)

    - also created internet gateway and attached to our VPC and in `public subnet route table`

    - create the` Nat gateway` associated with our public subnet and added the `private subnet route table` thru which it will access the internet.
