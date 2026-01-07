import os
import json
import datetime
import time
import yaml
import requests
import re
import gzip
import boto3
import graphyte
import tracemalloc
import logging
from pprint import pp


class LokiExporter:
    def __init__(self):

        # Configuration
        file_config = "conf/loki-export.yaml"
        self.__config = self.__load_config(file_config)

        # Init logging
        self.__logger = self.__init_logger()

        # State
        self.file_state = "conf/loki-export-state.json"
        self.dict_state = self.init_state()

        # Metric init
        self.__dict_metrics = dict()

        # Setting up S3 storage
        if "s3" in self.__config["storage"]:
            if self.__config['storage']['s3']['enabled'] is True:
                self.__logger.info(f"Export to s3 bucket {self.__config["storage"]["s3"]["bucket"]} is enabled")
                aws_session = boto3.Session(
                    aws_access_key_id=self.__config['storage']['s3']['aws_access_key_id'],
                    aws_secret_access_key=self.__config['storage']['s3']['aws_access_key_secret']
                )

                self.s3_connection = aws_session.resource("s3")
                self.s3_log_bucket = self.s3_connection.Bucket(self.__config["storage"]["s3"]["bucket"])

        if "local" in self.__config["storage"]:
            if self.__config['storage']['local']['enabled'] is True:
                self.__logger.info(f"Export to local path {self.__config["storage"]["local"]["path"]} is enabled")

        # Check Loki availability
        if not self.__check_loki():
            self.__logger.fatal(f"Loki is not ready on {self.__config['loki']['host']}")
            exit()

    def __check_loki(self):
        try_times = 3 # How often to try checking if Loki is ready
        url = f"{self.__config['loki']['host']}/ready"

        for x in range(0, try_times):
            try:
                self.__logger.debug(f"Verifying Loki connection")
                response = requests.get(url, verify=self.__config['requests_ca_bundle']).content.decode('utf-8').strip()
                if response == 'ready':
                    self.__logger.info("Loki is ready")
                    return True

                time.sleep(15) # It should take 15 seconds for Loki to get ready
            except Exception as e:
                raise SystemError(f"Can't connect to Loki on {self.__config['loki']['host']}.\n {e}")

        return False

    def __init_logger(self):
        this_logger = logging.getLogger()

        if not this_logger.hasHandlers():
            c_handler = logging.StreamHandler()
            if self.__config['stage'] == "dev":
                c_handler.setLevel(logging.DEBUG)
                this_logger.setLevel(logging.DEBUG)
            else:
                c_handler.setLevel(logging.INFO)
                this_logger.setLevel(logging.INFO)

            log_format = '%(asctime)s  %(levelname)-8s %(message)s'
            c_format = logging.Formatter(log_format, datefmt='%Y-%m-%d %H:%M:%S')
            c_handler.setFormatter(c_format)

            this_logger.addHandler(c_handler)

        return this_logger

    def init_state(self):
        if not os.path.exists(self.file_state):
            content = "{}"
            f_state = open(self.file_state, "w")
            f_state.write(content)
            self.__logger.info(f'State file {self.file_state} initialized')
        else:
            f_state = open(self.file_state, "r")
            content = f_state.read()
            self.__logger.info(f'State file {self.file_state} opened')

        f_state.close()

        try:
            return json.loads(content)
        except:
            raise Exception(f"Could not parse state file {self.file_state}")

    def __save_state(self, key, ts):
        self.dict_state[key] = ts

        with open(self.file_state, "w", encoding='latin-1') as f_state:
            json.dump(self.dict_state, f_state, indent=2)

    def __get_state(self, key):
        if key in self.dict_state:
            return self.dict_state[key]

    def __load_config(self, file_config):
        if not os.path.exists(file_config):
            raise FileNotFoundError("File " + file_config + " not found")

        with open(file_config) as f_config:
            config = yaml.safe_load(f_config)
            # json_config = json.load(f_config)
            return config

    def __inc_metric(self, metric, incr=1):
        if metric not in self.__dict_metrics:
            self.__dict_metrics[metric] = incr
        else:
            self.__dict_metrics[metric] += incr

    def __set_metric(self, metric, value):
        self.__dict_metrics[metric] = value

    def __get_metrics(self):
        return self.__dict_metrics

    def __send_metrics(self):
        graphite_host = self.__config["graphite_host"]
        graphite_prefix = self.__config["graphite_prefix"]

        graphyte.init(graphite_host, prefix=graphite_prefix)

        dict_metrics = self.__get_metrics()
        for key in dict_metrics:
            graphyte.send(key, dict_metrics[key])

    def __time_until_end_of_day(self, dt=None):
        """
        Get timedelta until end of day on the datetime passed, or current time.
        """
        if dt is None:
            dt = datetime.datetime.now()
        tomorrow = dt + datetime.timedelta(days=1)
        res = datetime.datetime.combine(tomorrow, datetime.time.min) - dt
        return res

    def __get_time_boundaries(self, obj_datetime_start):
        ts_start = str(int(obj_datetime_start.timestamp())) + "000000000"
        ts_end = str(int(obj_datetime_start.timestamp() + self.__time_until_end_of_day(
            obj_datetime_start).total_seconds())) + "000000000"

        return ts_start, ts_end

    def __get_logs(self, query, ts_start, ts_end):
        lst_log = list()

        # Query Loki for our logs
        loki_host = self.__config["loki"]["host"]
        if "max_lines_per_query" in self.__config:
            limit = "&limit=" + str(self.__config["max_lines_per_query"])
        else:
            limit = ""
        url = loki_host + "/loki/api/v1/query_range?query=" + query + "&start=" + ts_start + "&end=" + ts_end + \
            "&direction=forward" + limit

        self.__logger.debug(f'URL: {url}')
        result = requests.get(url, verify=self.__config['requests_ca_bundle'])
        dict_results = result.json()

        if dict_results["status"] == "success":
            if "result" in dict_results["data"]:
                if len(dict_results["data"]["result"]) > 0:
                    lst_log = dict_results["data"]["result"]

        return lst_log

    def __format_logs_to_elf(self, lst_logs):
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

        self.__logger.debug("Formatted list contains {0} lines".format(len(lst_formatted)))

        return "\n".join(lst_formatted)

    def __format_logs_to_json(self, lst_logs):
        # Append beginning and end curly braces
        str_logs = "{ \"values\": " + str(lst_logs[0]["values"]) + "}"

        # Replace single quotes with double quotes
        # Replace comma with \n
        str_logs = str_logs.replace("'", "\"")

        str_logs = json.dumps(json.loads(str_logs), indent=2)
        return str_logs

    def __export_logs(self, lst_logs, export_format, key, ts_day, iteration=1):

        # First, we format the logs
        # elf = Extended Log Format
        if export_format == "elf":
            str_logs = self.__format_logs_to_elf(lst_logs)
        else:
            # defaults to json (unmodified Loki format)
            str_logs = self.__format_logs_to_json(lst_logs)

        # Determine day
        obj_day = datetime.datetime.fromtimestamp(int(ts_day[:-9]))

        # Save the file to all designated output locations
        file_path = key + "/" + str(obj_day.year) + "/" + str(obj_day.month).zfill(2)
        # We can have multiple files per day, they are numbered using the iteration variable.
        file_name = (key + "-" + str(obj_day.year) +
                     str(obj_day.month).zfill(2) +
                     str(obj_day.day).zfill(2) +
                     "." + str(iteration).zfill(4) + ".log.gz")

        if "local" in self.__config["storage"] and self.__config["storage"]['local']['enabled'] is True:
            local_export_path = self.__config["storage"]["local"]["path"]
            full_path = local_export_path + "/" + file_path + "/" + file_name
            self.__logger.info("Local file to be written: " + full_path)
            if not os.path.exists(local_export_path + "/" + file_path):
                os.makedirs(local_export_path + "/" + file_path)

            # Create gzipped log file
            with gzip.open(full_path, 'wb') as f:
                f.write(bytes(str_logs, 'latin-1'))
                self.__logger.info(f"File {file_name} was written to {file_path}")
                self.__inc_metric("files-written.local")
                self.__inc_metric("lines-written.local", len(lst_logs[0]["values"]))

        if "s3" in self.__config["storage"] and self.__config["storage"]['s3']['enabled'] is True:
            # Store data temporarily
            tmp_file = "/tmp/" + file_name
            with gzip.open(tmp_file, 'wb') as f:
                f.write(bytes(str_logs, 'latin-1'))

            # Upload the file
            s3_path = file_path + "/" + file_name
            self.__logger.debug("S3 file to be written: " + s3_path)
            res = self.s3_log_bucket.upload_file(tmp_file, s3_path)

            # If all went OK, remove the file
            if res:
                self.__logger.info(f"S3 file {file_name} was written to {file_path}")
                self.__inc_metric("files-written.s3")
                self.__inc_metric("lines-written.s3", len(lst_logs[0]["values"]))
                os.remove(tmp_file)

    def __calculate_holdoff_timestamp(self):
        # Determining the amount of holdoff days
        holdoff_days = 0
        if self.__config["export_holdoff_days"]:
            holdoff_days = int(self.__config["export_holdoff_days"])

        # Create timestamp for today at 00:00:00
        ts_today = int(datetime.datetime.combine(datetime.date.today(), datetime.time()).timestamp())

        # Calculate holdoff timestamp
        ts_holdoff = str(ts_today - (holdoff_days * 86400)) + "000000000"
        return ts_holdoff

    def run(self):
        tracemalloc.start()
        time_start = time.perf_counter()

        max_lines = self.__config["max_lines_per_query"]
        if "max_days_per_exporter" in self.__config:
            max_days = int(self.__config["max_days_per_exporter"])
        else:
            max_days = 0

        for item in self.__config["exports"]:
            name = list(item.keys())[0]
            config = list(item.values())[0]

            if config["enabled"] is True:
                self.__logger.info(f"Processing export '{name}'")

                export_format = config["format"]
                key = re.sub(r"([{}\"])|[^a-z]", lambda m: "" if m.group(1) else "_", config["query"])

                ts_holdoff = self.__calculate_holdoff_timestamp()

                # Get start time from state
                state_time = self.__get_state(key)
                if state_time:
                    obj_time_start = datetime.datetime.fromtimestamp(int(state_time[:-9]))
                else:
                    obj_time_start = config["time_start"]

                # Get start and end time
                ts_start, ts_end = self.__get_time_boundaries(obj_time_start)
                self.__logger.debug(f"Start timestamp: {ts_start}")
                self.__logger.debug(f"End timestamp: {ts_end}")

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
                    lst_logs = self.__get_logs(config["query"], ts_start, ts_end)
                    batch_nr = 1

                    if len(lst_logs) > 0:

                        batch_size = len(lst_logs[0]["values"])
                        self.__logger.debug("Size of batch is {0}".format(batch_size))

                        while batch_size == max_lines:
                            # If size of returned log lines is equal to max lines, it can be assumed that
                            # there are more results available. Therefore, we have to 'page' our results, as indicated
                            # here: https://github.com/grafana/loki/issues/1625#issuecomment-582192791

                            # Export logs to storage
                            self.__export_logs(lst_logs, export_format, key, ts_start, batch_nr)

                            # Pick up timestamp from last log line
                            ts_start_interim = lst_logs[0]["values"][-1][0]

                            # Grab new batch
                            lst_logs = self.__get_logs(config["query"], ts_start_interim, ts_end)

                            if len(lst_logs) > 0:
                                batch_size = len(lst_logs[0]["values"])
                            else:
                                batch_size = 0

                            batch_nr += 1

                        # Export the tail end of logs
                        if len(lst_logs) > 0:
                            self.__export_logs(lst_logs, export_format, key, ts_start, batch_nr)
                            self.__logger.debug("Size of final batch is {0}".format(len(lst_logs[0]["values"])))

                        self.__logger.debug("Amount of batches: {0}".format(batch_nr))

                        self.__inc_metric("batches-sent", batch_nr)

                    else:
                        self.__logger.debug("Nothing to export. No logs for this day found.")

                    # store last timestamp
                    self.__save_state(key, ts_end)

                    # recalculate ts_start and ts_end, for the next day
                    ts_start, ts_end = self.__get_time_boundaries(datetime.datetime.fromtimestamp(int(ts_end[:-9])))
                    self.__logger.debug(f"Start timestamp: {ts_start}")
                    self.__logger.debug(f"End timestamp: {ts_end}")

        # Finally, collect performance metrics
        time_end = time.perf_counter()
        mem_usage = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        self.__set_metric("time-elapsed", time_end - time_start)
        self.__set_metric("memory-usage.min", mem_usage[0])
        self.__set_metric("memory-usage.max", mem_usage[1])

        self.__logger.info(self.__get_metrics())

        # Send metrics about this job
        if self.__config['stage'] != 'dev':
            pass
            # self.__send_metrics()


obj_exporter = LokiExporter()
obj_exporter.run()
