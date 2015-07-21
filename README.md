[![travis-ci.org](https://travis-ci.org/bsvingen/slack2pg.svg?branch=master)](https://travis-ci.org/bsvingen/slack2pg)

# slack2pg

## Introduction

This is a simple tool for storing Slack messages in PostgreSQL.

It uses the existing SQS integration provided by Slack, and then just
reads from this queue and writes to PostgreSQL.

There is also support for importing Slack export zip files.

Duplicate messages are automatically ignored.

## Configuration

Create a config file (assumed to be `config.edn` in the examples
below) according to the template provided in `config-template.edn`.

Provide the name of this file as the first argument on the command
line.

AWS credentials need to be available either through environment
variables or using EC2 instance profiles, in accordance with standard
Java AWS SDK use.

The database and database user referred to by the configuration needs
to exist - an easy way of creating them would be something like this:

```sql
create role slack login password ‘..';
create database slack owner slack;
```

## Building

To build, run

```bash
lein bin
```

You will then get the binary file `target/slack2pg`, which can be
moved somewhere convenient in your path.

## Creating the AWS stack

Do

```bash
lein bin config.edn aws-create
```

to create a CloudFormation stack containing the SQS queue and AWS
credentials that is to be provided to Slack on the SQS integration
configuration page.

The relevant values are provided as outputs from the stack, and
printed when this command runs.

(Notice that Slack requires the queue name, not the queue URL.)

Enter this into the Slack SQS integration web page to get things
started.

## Reading from the SQS queue

To read from the SQS queue:

```
slack2pg config.edn sqs
```

## Importing a Slack export zip file

To read from the Slack export zip file:

```
slack2pg config.edn read-export export_file.zip
```

