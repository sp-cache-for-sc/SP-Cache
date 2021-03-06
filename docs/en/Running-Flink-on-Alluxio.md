---
layout: global
title: Running Apache Flink on Alluxio
nickname: Apache Flink
group: Frameworks
priority: 2
---

* Table of Contents
{:toc}

This guide describes how to get Alluxio running with [Apache Flink](http://flink.apache.org/), so
that you can easily work with files stored in Alluxio.

## Prerequisites

The prerequisite for this part is that you have
[Java](Java-Setup.html). We also assume that you have set up
Alluxio in accordance to these guides [Local Mode](Running-Alluxio-Locally.html) or
[Cluster Mode](Running-Alluxio-on-a-Cluster.html).

Please find the guides for setting up Flink on the Apache Flink [website](http://flink.apache.org/).

## Configuration

Apache Flink allows to use Alluxio through a generic file system wrapper for the Hadoop file system.
Therefore, the configuration of Alluxio is done mostly in Hadoop configuration files.

### Set property in `core-site.xml`

If you have a Hadoop setup next to the Flink installation, add the following property to the
`core-site.xml` configuration file:

{% include Running-Flink-on-Alluxio/core-site-configuration.md %}

In case you don't have a Hadoop setup, you have to create a file called `core-site.xml` with the
following contents:

{% include Running-Flink-on-Alluxio/create-core-site.md %}

### Specify path to `core-site.xml` in `conf/flink-conf.yaml`

Next, you have to specify the path to the Hadoop configuration in Flink. Open the
`conf/flink-conf.yaml` file in the Flink root directory and set the `fs.hdfs.hadoopconf`
configuration value to the **directory** containing the `core-site.xml`. (For newer Hadoop versions,
the directory usually ends with `etc/hadoop`.)

### Generate and Distribute the Alluxio Client Jar

In order to communicate with Alluxio, we need to provide Flink programs with the Alluxio Core Client
jar.

There are different ways to achieve that:

- Put the `{{site.ALLUXIO_CLIENT_JAR_PATH_FLINK}}` file into the `lib` directory of Flink (for local and
standalone cluster setups)
- Put the `{{site.ALLUXIO_CLIENT_JAR_PATH_FLINK}}` file into the `ship` directory for Flink on YARN.
- Specify the location of the jar file in the `HADOOP_CLASSPATH` environment variable (make sure its
available on all cluster nodes as well). For example like this:

{% include Running-Flink-on-Alluxio/hadoop-classpath.md %}

Alternatively, advanced users can choose to compile this client jar from the source code. Follow the instructs [here](Building-Alluxio-Master-Branch.html#compute-framework-support) and use the generated jar at `{{site.ALLUXIO_CLIENT_JAR_PATH_BUILD}}` for the rest of this guide.

### Translate additional Alluxio site properties to Flink

In addition, if there are any properties specified in `conf/alluxio-site.properties`,
translate those to `env.java.opts` in `{FLINK_HOME}/conf/flink-conf.yaml` for Flink to pick up
Alluxio configuration.

## Using Alluxio with Flink

To use Alluxio with Flink, just specify paths with the `alluxio://` scheme.

If Alluxio is installed locally, a valid path would look like this
`alluxio://localhost:19998/user/hduser/gutenberg`.

### Wordcount Example

This example assumes you have set up Alluxio and Flink as previously described.

Put the file `LICENSE` into Alluxio, assuming you are in the top level Alluxio project directory:

{% include Running-Flink-on-Alluxio/license.md %}

Run the following command from the top level Flink project directory:

{% include Running-Flink-on-Alluxio/wordcount.md %}

Open your browser and check [http://localhost:19999/browse](http://localhost:19999/browse). There should be an output file `output` which contains the word counts of the file `LICENSE`.
