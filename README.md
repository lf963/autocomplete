# Autocomplete

Utilize MapReduce on Hadoop to implement autocomplete

## Getting Started

### Prerequisites

What things you need to install

```
Docker, Hadoop, PHP, MySQL
```

### Installing Docker and Hadoop

Install Docker

```
$ sudo apt-get update
$ sudo apt-get install wget
$ sudo curl -sSL https://get.daocloud.io/docker | sh
$ sudo docker info
```

Install Hadoop based on Docker

```
$ mkdir bigdata-class2
$ cd bigdata-class2
$ sudo docker pull joway/hadoop-cluster # pull docker image from dockerhub
$ git clone https://github.com/joway/hadoop-cluster-docker # clone code from github
$ sudo docker network create --driver=bridge hadoop #create hadoop network
$ cd hadoop-cluster-docker
```

Enter Docker

```
$ sudo ./start-container.sh
```

Start Hadoop in docker

```
$ sudo ./start-hadoop.sh
```

### MySQL Configuration

Create a database and table

```
create database test;
use test;
create table output(starting_phrase VARCHAR(250), following_word VARCHAR(250), count INT);
```

Grant all privileges so that hadoop can connect to your database
Remeber to substitude the 'password' to your password

```
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' IDENTIFIED BY 'password' WITH GRANT OPTION;
FLUSH PRIVILEGES;
```

Get the port of your MySQL, we will use this port when connecting hadoop and MySQL

```
SHOW VARIABLES WHERE Variable_name = 'port' ;
```


### Setup Hadoop

Change directory to hadoop-cluster-docker (last step of "Install Hadoop based on Docker")

```
$ cd hadoop-cluster-docker
```

Start docker container and hadoop

```
$ sudo ./start-container.sh
$ sudo ./start-hadoop.sh
```

In HDFS, put mysql-connector-java-5.1.39-bin.jar into a directory

```
$ hdfs dfs -mkdir /mysql	#make a directory named /mysql in HDFS
$ hdfs dfs -put mysql-connector-java-*.jar /mysql/
```

In HDFS, put bookList(our input) into a directory

```
# hdfs dfs -mkdir /input
$ hdfs dfs -rm -r /output	#MapReduce will produce an output directory automatically
                            #Everytime starting MapReduce, remember to remove output directory
$ hdfs dfs -put bookList/*  input/ 
```

Go to Driver.java and modify MySQL connection information

```
DBConfiguration.configureDB(conf2, 
				"com.mysql.jdbc.Driver",
				"jdbc:mysql://ip address:port/database name",
				"root",
				"password");
job2.addArchiveToClassPath(new Path("connector's path"));	#which is /mysql
```

## Running MapReduce

```
$ hadoop com.sun.tools.javac.Main *.java
$ jar cf ngram.jar *.class
$ hadoop jar ngram.jar Driver /input /output 2 3 4 
#2: ngram_size 
#3: threshold, if a phrase appears less than threshold, ignore it
#4: topK
#2 3 4 can be set randomly
```

If it runs successfully, we can see information is written into database.
