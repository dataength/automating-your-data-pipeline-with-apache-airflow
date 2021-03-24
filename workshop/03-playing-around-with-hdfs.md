## Playing Around with HDFS

### Listing the directory on HDFS

```sh
hdfs dfs -ls /
```

### Uploading a file to HDFS

```sh
hdfs dfs -put data.txt /
```

### Getting the basic file system information and statistics about the HDFS

```sh
hdfs dfsadmin -report
```