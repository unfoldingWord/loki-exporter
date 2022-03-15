# Loki Exporter

Loki Exporter pulls out data from Loki, using the official API. 
It can be used as a kind of backup tool.

## Description
Loki exporter queries Loki and stores the results in gzipped files, 
either locally or on S3. 
Most of its behaviour is defined through the file `conf/loki-export.conf`.
which is in JSON format.

**Example of `loki-export.conf`**
```json
{
  "loki_host": "http://localhost:3100",
  "graphite_host": "localhost",
  "graphite_prefix": "loki.backup",
  "max_lines_per_query": 5000,
  "max_days_per_exporter": 100,
  "storage": {
    "local": {
      "filepath": "loki_export"
    },
    "s3": {
      "aws_bucket": "loki-export.yourorg.com"
    }
  },
  "exporters": [
    {
      "active": true,
      "query": "{system=\"server1\",app=\"nginx\"}",
      "time_start": "2020-02-23 00:00:00",
      "format": "elf"
    },
    {
      "active": false,
      "query": "{system=\"server2\",app=\"forum\"}",
      "time_start": "2020-03-13 00:00:00",
      "format": "elf"
    }
  ]
}
```
**Global settings**
- `loki_host`: the URL that Loki can be reached on
- `graphite_host`: the URL that Graphite can be reached on. Graphite is used for storing metrics.
- `graphite_prefix`: the prefix for all Graphite metrics
- `max_lines_per_query`: How many lines we pull in per query. 5000 seems to be the max for Loki.
- `max_days_per_exporter`: For each run, how many days we will import per exporter. 

**Storage settings**

You can have multiple storage settings, and they will all be used simultaneously. So you can store your data both locally and in S3.
Currently, we only support `local` and `s3`
- `local`
  - `filepath`: The path where to store the exported files. If this directory does not exist, we will try to create it.
- `s3`
  - `aws_bucket`: The name of the bucket where files will be stored. Make sure that you have at least write access to this bucket.

**Exporters**

Each exporter defines the query that will pull the desired data out of Loki
- `active`: If this exporter can be used or not. Having `"active": false` exporters gives you the opportunity to preconfigure exporters that are not in Loki yet.
- `query`: This is simply the LogQL query to fetch the required data from Loki
- `time_start`: The start date to use in the query. This start time is only used at the very 
beginning of the export. The start time for subsequent queries will be pulled from the file `conf/loki-export-state.json`.
- `format`: This can be either `elf` (Extended Log Format) or `json`. 

## Getting Started

### Dependencies

For Python dependencies, see `requirements.txt`.

As a minimum, you need to setup a graphite server and a Loki server
For easy testing and/or deployment, use the following docker containers:
- [graphiteapp/graphite-statsd](https://hub.docker.com/r/graphiteapp/graphite-statsd)
- [grafana/loki](https://hub.docker.com/r/grafana/loki)

Depending on your configuration, you might also need to setup a bucket in S3 with the correct permissions. 

### Installing

- Clone this repository
```
git clone git@github.com:unfoldingWord-dev/loki-exporter
cd 
pip install -r requirements.txt
```

- Or pull the docker container from [here](https://hub.docker.com/r/unfoldingword/loki-exporter)
```
docker pull unfoldingword/loki-exporter
```

- Or build your own docker container with the help of the provided Dockerfile
```
docker build -t <dockerhub-username>/<repo-name> .
```

### Executing program
#### Running the python program
```
python path/to/repository/main.py
```

#### Running as a docker container
When you run it as a container, you need to bind-mount at least a configuration directory
Also, when you opt for export to local storage, you need to provide the backup directory
```
docker run --env-file .env --rm --net=host --mount type=bind,source="$(pwd)"/loki-backup/conf,target=/app/conf --mount type=bind,source="$(pwd)"/loki-backup/loki_export,target=/app/loki_export --name loki-exporter unfoldingword/loki-exporter
```

You need to provide the following environment variables, 
either through a .env file, or by setting them manually

- `AWS_ACCESS_KEY_ID`: *your AWS Access Key ID*
- `AWS_ACCESS_KEY_SECRET`: *your AWS Access Key Secret*
- `STAGE`: Are you running on `dev` or `prod`? On `dev`, we are a bit more verbose with logging.

## Authors

- [yakob-aleksandrovich ](https://github.com/yakob-aleksandrovich)

## Version History

* 0.1
    * Initial Release

## License

This project is licensed under the MIT License