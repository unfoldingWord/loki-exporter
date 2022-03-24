import os
import json
import datetime
import time
import dotenv
import requests
import re
import gzip
import boto3
import graphyte
import tracemalloc
import logging


class LokiExporter:
    def __init__(self):
        dotenv.load_dotenv()

        # Init logging
        if os.getenv("STAGE") == "dev":
            log_level = logging.DEBUG
        else:
            # stage == prod
            log_level = logging.INFO

        logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger("global").getChild("loki-export-logger")
        self.logger.setLevel(log_level)

        # Configuration
        file_config = "conf/loki-export.conf"
        self.config = self.load_config(file_config)

        # State
        self.file_state = "conf/loki-export-state.json"
        self.dict_state = self.init_state()

        # Metric setup
        self.graphite_host = os.getenv("GRAPHITE_HOST")
        self.graphite_prefix = os.getenv("GRAPHITE_PREFIX")
        self.dict_metrics = dict()

        # Storage
        if "s3" in self.config["storage"]:
            aws_session = boto3.Session(
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_ACCESS_KEY_SECRET")
            )

            self.s3_connection = aws_session.resource("s3")
            self.s3_log_bucket = self.s3_connection.Bucket(self.config["storage"]["s3"]["aws_bucket"])

    def init_state(self):
        if not os.path.exists(self.file_state):
            content = "{}"
            f_state = open(self.file_state, "w")
            f_state.write(content)
        else:
            f_state = open(self.file_state, "r")
            content = f_state.read()

        f_state.close()

        return json.loads(content)

    def save_state(self, key, ts):
        self.dict_state[key] = ts

        with open(self.file_state, "w", encoding='latin-1') as f_state:
            json.dump(self.dict_state, f_state, indent=2)

    def get_state(self, key):
        if key in self.dict_state:
            return self.dict_state[key]

    def load_config(self, file_config):
        if not os.path.exists(file_config):
            raise FileNotFoundError("File " + file_config + " not found")

        with open(file_config) as f_config:
            json_config = json.load(f_config)
            return json_config

    def inc_metric(self, metric, incr=1):
        if metric not in self.dict_metrics:
            self.dict_metrics[metric] = incr
        else:
            self.dict_metrics[metric] += incr

    def set_metric(self, metric, value):
        self.dict_metrics[metric] = value

    def get_metrics(self):
        return self.dict_metrics

    def send_metrics(self):
        graphite_host = self.config["graphite_host"]
        graphite_prefix = self.config["graphite_prefix"]

        graphyte.init(graphite_host, prefix=graphite_prefix)

        dict_metrics = self.get_metrics()
        for key in dict_metrics:
            graphyte.send(key, dict_metrics[key])

    def time_until_end_of_day(self, dt=None):
        """
        Get timedelta until end of day on the datetime passed, or current time.
        """
        if dt is None:
            dt = datetime.datetime.now()
        tomorrow = dt + datetime.timedelta(days=1)
        res = datetime.datetime.combine(tomorrow, datetime.time.min) - dt
        return res

    def get_time_boundaries(self, obj_datetime_start):
        ts_start = str(int(obj_datetime_start.timestamp())) + "000000000"
        ts_end = str(int(obj_datetime_start.timestamp() + self.time_until_end_of_day(
            obj_datetime_start).total_seconds())) + "000000000"

        return ts_start, ts_end

    def get_logs(self, query, ts_start, ts_end):
        lst_log = list()

        # Query Loki for our logs
        loki_host = self.config["loki_host"]
        if "max_lines_per_query" in self.config:
            limit = "&limit=" + str(self.config["max_lines_per_query"])
        else:
            limit = ""
        url = loki_host + "/loki/api/v1/query_range?query=" + query + "&start=" + ts_start + "&end=" + ts_end + \
            "&direction=forward" + limit
        # print(url)
        result = requests.get(url)
        dict_results = result.json()

        if dict_results["status"] == "success":
            if "result" in dict_results["data"]:
                if len(dict_results["data"]["result"]) > 0:
                    lst_log = dict_results["data"]["result"]

        return lst_log

    def format_logs_to_elf(self, lst_logs):
        lst_formatted = list()
        for line in lst_logs[0]["values"]:
            # Format timestamp into readable date and time
            # Remove the last 9 numbers from timestamp (microseconds part)
            ts = int(line[0][:-9])
            date = datetime.datetime.fromtimestamp(ts).strftime("%Y-%m-%d\t%H:%M:%S")

            # Format actual log message: replace spaces with tabs, except when within double quotes
            regex = r'"[^"]+"|(\s)'
            msg = re.sub(regex, lambda m: "\t" if m.group(1) else m.group(0), line[1])

            lst_formatted.append(date + "\t" + msg)

        self.logger.debug("Formatted list contains {0} lines".format(len(lst_formatted)))

        return "\n".join(lst_formatted)

    def format_logs_to_json(self, lst_logs):
        # Append beginning and end curly braces
        str_logs = "{ \"values\": " + str(lst_logs[0]["values"]) + "}"

        # Replace single quotes with double quotes
        # Replace comma with \n
        str_logs = str_logs.replace("'", "\"")

        str_logs = json.dumps(json.loads(str_logs), indent=2)
        return str_logs

    def export_logs(self, lst_logs, export_format, key, ts_day, iteration=1):
        # First, we format the logs
        # elf = Extended Log Format
        if export_format == "elf":
            str_logs = self.format_logs_to_elf(lst_logs)

        # defaults to json (unmodified Loki format)
        else:
            str_logs = self.format_logs_to_json(lst_logs)

        # Determine day
        obj_day = datetime.datetime.fromtimestamp(int(ts_day[:-9]))

        # Save the file to all designated output locations
        file_path = key + "/" + str(obj_day.year) + "/" + str(obj_day.month).zfill(2)
        # We can have multiple files per day, they are numbered using the iteration variable.
        file_name = key + "-" + str(obj_day.year) + str(obj_day.month).zfill(2) + str(obj_day.day).zfill(2) + \
            "." + str(iteration).zfill(4) + ".log.gz"

        if "local" in self.config["storage"]:
            local_export_path = self.config["storage"]["local"]["filepath"]
            full_path = local_export_path + "/" + file_path + "/" + file_name
            self.logger.debug("Local file to be written: " + full_path)
            if not os.path.exists(local_export_path + "/" + file_path):
                os.makedirs(local_export_path + "/" + file_path)

            # Create gzipped log file
            with gzip.open(full_path, 'wb') as f:
                f.write(bytes(str_logs, 'latin-1'))
                self.inc_metric("files-written.local")
                self.inc_metric("lines-written.local", len(lst_logs[0]["values"]))

        if "s3" in self.config["storage"]:
            # Store data temporarily
            tmp_file = "/tmp/" + file_name
            with gzip.open(tmp_file, 'wb') as f:
                f.write(bytes(str_logs, 'latin-1'))

            # Upload the file
            s3_path = file_path + "/" + file_name
            self.logger.debug("S3 file to be written: " + s3_path)
            res = self.s3_log_bucket.upload_file(tmp_file, s3_path)

            # If all went OK, remove the file
            if res:
                self.inc_metric("files-written.s3")
                self.inc_metric("lines-written.s3", len(lst_logs[0]["values"]))
                os.remove(tmp_file)

    def __calculate_holdoff_timestamp(self):
        # Determining the amount of holdoff days
        holdoff_days = 0
        if self.config["export_holdoff_days"]:
            holdoff_days = int(self.config["export_holdoff_days"])

        # Create timestamp for today at 00:00:00
        ts_today = int(datetime.datetime.combine(datetime.date.today(), datetime.time()).timestamp())

        # Calculate holdoff timestamp
        ts_holdoff = str(ts_today - (holdoff_days * 86400)) + "000000000"
        return ts_holdoff

    def run(self):
        tracemalloc.start()
        time_start = time.perf_counter()

        max_lines = self.config["max_lines_per_query"]
        if "max_days_per_exporter" in self.config:
            max_days = int(self.config["max_days_per_exporter"])
        else:
            max_days = 0

        for item in self.config["exporters"]:
            if item["active"] is True:
                export_format = item["format"]
                key = re.sub(r"([{}\"])|[^a-z]", lambda m: "" if m.group(1) else "_", item["query"])

                ts_holdoff = self.__calculate_holdoff_timestamp()

                # Try to fetch the start time from the state file
                state_time = self.get_state(key)
                if state_time:
                    obj_time_start = datetime.datetime.fromtimestamp(int(state_time[:-9]))
                else:
                    obj_time_start = datetime.datetime.strptime(item["time_start"], "%Y-%m-%d %H:%M:%S")

                # Get start and end time
                ts_start, ts_end = self.get_time_boundaries(obj_time_start)

                # We fetch the logs per day
                # If start_time in current day, then stop (we only export through 'yesterday')
                day_counter = 0
                while ts_start < ts_holdoff:
                    # Maximum number of days per run (to keep Loki memory in check)
                    day_counter += 1
                    if not max_days == 0:
                        if day_counter > max_days:
                            break

                    # Initial batch
                    lst_logs = self.get_logs(item["query"], ts_start, ts_end)
                    batch_nr = 1

                    if len(lst_logs) > 0:

                        batch_size = len(lst_logs[0]["values"])
                        self.logger.debug("Size of batch is {0}".format(batch_size))

                        while batch_size == max_lines:
                            # If size of returned log lines is equal to max lines, it can be assumed that
                            # there are more results available. Therefore, we have to 'page' our results, as indicated
                            # here: https://github.com/grafana/loki/issues/1625#issuecomment-582192791

                            # Export logs to storage
                            self.export_logs(lst_logs, export_format, key, ts_start, batch_nr)

                            # Pick up timestamp from last log line
                            ts_start_interim = lst_logs[0]["values"][-1][0]

                            # Grab new batch
                            lst_logs = self.get_logs(item["query"], ts_start_interim, ts_end)

                            if len(lst_logs) > 0:
                                batch_size = len(lst_logs[0]["values"])
                            else:
                                batch_size = 0

                            batch_nr += 1

                        # Export the tail end of logs
                        if len(lst_logs) > 0:
                            self.export_logs(lst_logs, export_format, key, ts_start, batch_nr)
                            self.logger.debug("Size of final batch is {0}".format(len(lst_logs[0]["values"])))

                        self.logger.debug("Amount of batches: {0}".format(batch_nr))

                        self.inc_metric("batches-sent", batch_nr)

                    # store last timestamp
                    self.save_state(key, ts_end)

                    # recalculate ts_start and ts_end, for the next day
                    ts_start, ts_end = self.get_time_boundaries(datetime.datetime.fromtimestamp(int(ts_end[:-9])))

        # Finally, collect performance metrics
        time_end = time.perf_counter()
        mem_usage = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        self.set_metric("time-elapsed", time_end - time_start)
        self.set_metric("memory-usage.min", mem_usage[0])
        self.set_metric("memory-usage.max", mem_usage[1])

        self.logger.info(self.get_metrics())

        # Send metrics about this job
        self.send_metrics()


obj_exporter = LokiExporter()
obj_exporter.run()
