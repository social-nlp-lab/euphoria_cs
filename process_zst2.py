import pandas as pd
import zstandard
import os
import json
import sys
from datetime import datetime
import logging.handlers
log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

# only output items between these two dates
from_date = datetime.strptime("2019-06-01", "%Y-%m-%d")
to_date = datetime.strptime("2022-12-31", "%Y-%m-%d")

def process_file(file_path, field, value, from_date=None, to_date=None):
    file_size = os.stat(file_path).st_size
    file_lines = 0
    file_bytes_processed = 0
    created = None
    bad_lines = 0
    data = []

    def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
        chunk = reader.read(chunk_size)
        bytes_read += chunk_size
        if previous_chunk is not None:
            chunk = previous_chunk + chunk
        try:
            return chunk.decode()
        except UnicodeDecodeError:
            if bytes_read > max_window_size:
                raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
            log.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
            return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

    def read_lines_zst(file_name):
        with open(file_name, 'rb') as file_handle:
            buffer = ''
            reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
            while True:
                chunk = read_and_decode(reader, 2**27, (2**29) * 2)

                if not chunk:
                    break
                lines = (buffer + chunk).split("\n")

                for line in lines[:-1]:
                    yield line, file_handle.tell()

                buffer = lines[-1]

            reader.close()

    for line, file_bytes_processed in read_lines_zst(file_path):
        try:
            obj = json.loads(line)
            created = datetime.utcfromtimestamp(int(obj['created_utc']))
            if from_date and created < from_date:
                continue
            if to_date and created > to_date:
                continue
            temp = obj[field] == value
            data.append(obj)
        except (KeyError, json.JSONDecodeError) as err:
            bad_lines += 1
        file_lines += 1
        if file_lines % 100000 == 0:
            log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {bad_lines:,} : {file_bytes_processed:,}:{(file_bytes_processed / file_size) * 100:.0f}%")

    df = pd.DataFrame(data)
    return df