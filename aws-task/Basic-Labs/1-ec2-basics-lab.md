# EC2 Basics Lab

- `Objective`: To understand the process of setting up and managing an Amazon EC2 instance.

- `Approach`: Students will start by launching a new EC2 instance, selecting an appropriate instance type and configuring the instance details. They will then create and configure a new Security Group, and allocate an Elastic IP address to the instance. The lab will also include connecting to the instance via SSH.

- `Goal` : By the end of this lab, students should be able to launch and manage an EC2 instance, understand instance types, security groups, and IP addressing in AWS.



## Amazon EC2

- Infrastructure as a service (Iaas)
- Instance based
- Virtual machnies (referred to as EC2 instances)
- Provision virtual machines that you can manage as you choose.
- Lauch instances from Amazon machine Images (AMI)
- you can control traffic to and from instances by using security group.

### 9 key decisions to make when you create an EC2 instance

- 1. **`AMI`** (Amaon Machine Image)
    - template that is used to create an instance (contains windows or linux operating system)
    - often also has some software pre-installed

      
- 2. **`Instance Type`**
    - the instance type that you choose determines 
        - Memory(RAM)
        - Processing power (CPU)
        - Disk space and disk type (Storage)
        - Network performance
          
     ![compute2](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/5bbe2503-5086-4f20-a985-7237d6f71621)

     ![compute3](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/a56b6094-730d-42a2-b079-21a8a77e5bda)

- 3. **`Network settings`**
    - Network location where EC2 should be deployed.
    - Where should the instance be deployed?
        - Identify the VPC and optionlly the subnet.(if not assigned i will deployed in default VPC)
    - Should a public IP address be automatically assigned?
        - To make it internet-accessible.
          
- 4. **`IAM role`**
    - Will software on the EC2 instance need to interact with other AWS services ?
        - if yes, attach an appropriate IAM Role
    - An AWS identity and Access Management (IAM) role that is `attached `to an `EC2 instance `is kept in an `instance profile`.
    - you can also attach a role to instance that already exists.
      
      ![compute4](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/501a8178-b944-4519-9cd9-91fe49004047)

- 5. **`User data`**
    - Optionally specify a user data script at intance launch.
    - Use `user data` scripts to customize the runtime environment of your instance.
        - Script executes the first time the instance starts
          
    ![compute5](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/fde73237-50d1-4f6b-981b-6d687a61d7ce)

- 6. **`Storage options`**
    - Configure the `root volume`
        - where the guest OS is installed
    - For each volume,specify
        - the size of the disk (in GB)
        - the volume type
            - SSDs or HDDs
        - the volume will be deleted when the instance is terminated
    - Storage options
        - Amazon Elastic Block Store (amazon EBS)
            - block-level storage volumes
            - `you can stop the instance and star it again,and the data will still be there `
        - Amazon EC2 `Instance Store`
            - storage is provided on disks that are attached to the host computer where the EC2 instance is running.
            - `if the instance stops,data stored here is deleted.`
        - Other options for storage(not for the root volume)
            - Mount an Amazon Elastic File System (Amazon EFS)
            - Connect to Amazon Simple Storage Service (Amazon S3)
              
![compute6](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/91aa22de-9adb-4432-b6e3-6441638ced61)


- 7. **`Tags`**
    - A tag is label that you can assign to an AWS resource.
        - consists of a key and an optional value
    - Tagging is how you can attach metadata to an EC2 instance.

- 8. **`Security group`**
    - A security group is a set of firewall rules that control traffice to the instance
    
- 9. **`Key Pair`**
    - At instance launch , you specify an existing key pair or create a new key pair
    - A key pair consists of
        - A `public key `that `aws stores `
        - A `private key file` (Eg .pem file) that `you store`
    - It enables secure connections to the instance
    - For Windows AMIs
        - use the private key to obtain the admin password that you need to log in to your instance
    - For Linux AMIs
        - use the private key to use SSH to securely connect to you instance
    

### Another option to launch an EC2 instance with AWS CLI

![compute7](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/eabd4ab1-910b-49c2-a4be-b40d55e2a79f)


### Amazon EC2 instance lifecycle

![compute8](https://github.com/anupmaharzn/intro-to-aws/assets/34486226/780bc146-687f-4b6f-9eeb-2955ab8ce122)


### Consider using an Elastic IP address

- `Rebooting `an instance will `not change `any IP address or DNS hostnames
- when an instance is `stopped `and then `started `again
    - `the public ipv4 address and exteranl DNS hostname will change`
    - `the private ipv4 address and internal DNS hostname do not change.`

- If you require a `persistent public IP address`
    - Associate an `Elastic IP address `with the instance.

- Elastic IP address characteristics
    - can be associated with instances in the Region as needed.
    - reamins allocated to you account until you choose to release it.

### EC2 instance metadata
- instance metadata is data about your instance
- while you are conneted to the instance, you can view it
    - in a browser: `` http://169.254.169.254/latest/meta-data/``
    - in a terminal window: ``curl http://169.254.169.254/latest/meta-data/``


## Screenshots of AWS Task

![basic-ec2-1](https://github.com/anupmaharzn/aws-task/assets/34486226/15068aaf-7017-4b38-9ee2-0b3620c4afc0)

- **Create Instance**
  
![basic-ec2-2](https://github.com/anupmaharzn/aws-task/assets/34486226/9757174c-4fb2-4512-8a86-1e0dfec10b12)

- **We can see Instance is Created**

![basic-ec2-3](https://github.com/anupmaharzn/aws-task/assets/34486226/b779a52a-7c34-49f1-b3ea-0b51758f5eb7)

- **Connect Instance with SSH**
  
![basic-ec2-4](https://github.com/anupmaharzn/aws-task/assets/34486226/a087552f-ed15-45b6-8006-7d881f8e29ce)

