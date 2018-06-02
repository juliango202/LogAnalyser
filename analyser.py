import csv
import logging
import datetime

import datastructures

LOG_PROGRESS_EVERY_N_LINES = 10000


class LogAnalyser(object):
    """Read a TSV-formatted log with (timestamp,query_text) columns and record data
    in a custom Trie for fast querying"""
    def __init__(self):
        self.trie = datastructures.Trie()

    def process_log_file(self, log_file):
        logging.info(f"Processing log file {log_file}...")
        with open(log_file) as csv_file:
            tsv_reader = csv.reader(csv_file, delimiter='\t')
            line = 1
            for row in tsv_reader:
                if len(row) != 2:
                    logging.warning(f"Line {line} does not have 2 columns! It will be ignored.")
                    continue

                # Column 0 => timestamp
                timestamp = row[0].strip()
                try:
                    datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    logging.warning(f"Line {line} timestamp {timestamp} is invalid! It will be ignored.")
                    continue

                # Column 1 => query_text
                query_text = row[1].strip()
                if not query_text:
                    logging.warning(f"Line {line} search query is empty! It will be ignored.")
                    continue

                self.trie.add(timestamp, query_text)

                if line % LOG_PROGRESS_EVERY_N_LINES == 0:
                    logging.info(f"Read {line} lines")
                line += 1
        logging.info(f"Finished processing log file {log_file}.\n")
        self.trie.finalize()

    def distinct_queries_by_prefix(self, prefix):
        count = self.trie.distinct_queries_by_prefix(prefix)
        return {"count": count}

    def top_queries_by_prefix(self, prefix, size):
        top_queries = self.trie.top_queries_by_prefix(prefix, size)
        return {"queries": [{"query": tq[0], "count": tq[1]} for tq in top_queries]}
