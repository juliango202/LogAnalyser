import logging
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, unquote

from analyser import LogAnalyser
from datastructures import InvalidQuerySize, InvalidDatePrefix

HN_LOGS_FILE = "hn_logs.tsv"
COUNT_QUERY_PATH = "/1/queries/count/"
TOP_QUERY_PATH = "/1/queries/popular/"
TOP_QUERY_SIZE_PARAM = "size"


def make_handler(log_analyser):
    """A small web server for our log analyser demo
    """
    class LogAnalyserHandler(BaseHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            self.log_analyser = log_analyser
            super(LogAnalyserHandler, self).__init__(*args, **kwargs)

        def get_path_parts(self):
            """Return the request base_path(path without query string) and the query string dictionary"""
            if "?" not in self.path:
                return self.path, None
            parts = self.path.split("?")
            return parts[0], parse_qs(parts[1])

        def do_GET(self):
            try:
                if self.path.startswith(COUNT_QUERY_PATH):
                    self.get_distinct_queries()
                elif self.path.startswith(TOP_QUERY_PATH):
                    self.get_top_queries()
                else:
                    self.send_error(404)
            except (InvalidQuerySize, InvalidDatePrefix) as exc:
                self.send_error(400, str(exc))

        def get_distinct_queries(self):
            date_prefix = unquote(self.path.split("/")[-1])
            data = self.log_analyser.distinct_queries_by_prefix(date_prefix)
            self.send_in_json(data)

        def get_top_queries(self):
            base_path, query_string = self.get_path_parts()
            if not query_string or TOP_QUERY_SIZE_PARAM not in query_string:
                self.send_error(400, f"The '{TOP_QUERY_SIZE_PARAM}' query string parameter is missing")
                return
            if not query_string[TOP_QUERY_SIZE_PARAM][0].isnumeric():
                self.send_error(400, f"The '{TOP_QUERY_SIZE_PARAM}' query string parameter is missing")
                return
            date_prefix = unquote(base_path.split("/")[-1])
            size = int(query_string[TOP_QUERY_SIZE_PARAM][0])
            data = self.log_analyser.top_queries_by_prefix(date_prefix, size)
            self.send_in_json(data)

        def send_in_json(self, data):
            """Send data encoded as JSON"""
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(data).encode("utf-8"))

    return LogAnalyserHandler


def run(port=8080):
    logging.basicConfig(level=logging.INFO)

    log_analyser = LogAnalyser()
    log_analyser.process_log_file(HN_LOGS_FILE)
    run_tests(log_analyser)

    handler_class = make_handler(log_analyser)
    server_address = ("", port)
    httpd = HTTPServer(server_address, handler_class)
    logging.info("Starting httpd...\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    logging.info("Stopping httpd...\n")


def run_tests(log_analyser):
    assert log_analyser.distinct_queries_by_prefix("2015")["count"] == 573697
    assert log_analyser.distinct_queries_by_prefix("2015-08")["count"] == 573697
    assert log_analyser.distinct_queries_by_prefix("2015-08-03")["count"] == 198117
    assert log_analyser.distinct_queries_by_prefix("2015-08-01 00:04")["count"] == 617

    assert log_analyser.top_queries_by_prefix("2015", 3) == {
      "queries": [
        {"query": "http%3A%2F%2Fwww.getsidekick.com%2Fblog%2Fbody-language-advice", "count": 6675},
        {"query": "http%3A%2F%2Fwebboard.yenta4.com%2Ftopic%2F568045", "count": 4652},
        {"query": "http%3A%2F%2Fwebboard.yenta4.com%2Ftopic%2F379035%3Fsort%3D1", "count": 3100}
      ]
    }

    assert log_analyser.top_queries_by_prefix("2015-08-02", 5) == {
      "queries": [
        {"query": "http%3A%2F%2Fwww.getsidekick.com%2Fblog%2Fbody-language-advice", "count": 2283},
        {"query": "http%3A%2F%2Fwebboard.yenta4.com%2Ftopic%2F568045", "count": 1943},
        {"query": "http%3A%2F%2Fwebboard.yenta4.com%2Ftopic%2F379035%3Fsort%3D1", "count": 1358},
        {"query": "http%3A%2F%2Fjamonkey.com%2F50-organizing-ideas-for-every-room-in-your-house%2F", "count": 890},
        {"query": "http%3A%2F%2Fsharingis.cool%2F1000-musicians-played-foo-fighters-learn-to-fly-and-it-was-epic", "count": 701}
      ]
    }


if __name__ == "__main__":
    from sys import argv
    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()