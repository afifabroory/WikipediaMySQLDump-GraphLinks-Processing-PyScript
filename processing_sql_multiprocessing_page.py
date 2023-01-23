# Code optimized for category

import gzip
import time
import itertools
import multiprocessing
import asyncio
import aiofiles

async def write_data(filename, data):
    async with aiofiles.open(filename, "a") as f:
        await f.write(data)

def process_line(data):
    # Variable to hold buffer
    buffer_ns0_0redirect = ""
    buffer_ns0_1redirect = ""
    buffer_ns14_0redirect = ""
    buffer_ns14_1redirect = ""

    total_record_processed = 0
    for line in data:
        splitted = line.decode("utf-8", errors="replace").split(" ")

        # Filter data only INSERT statement
        if splitted[0] != "INSERT":
            continue

        # Split by '),('. The only reliable way to extract the data
        data = splitted[-1].split(",NULL),(")

        try:
            for i, record in enumerate(data):
                buffer = ""
                for j, chunk in enumerate(record.split(",", 1)):
                    # page_id
                    if j == 0:
                        # If chunk are first in line.
                        if i == 0:
                            buffer += chunk[1:] + ","
                        else:
                            buffer += chunk + ","
                        continue
                    
                    # page_namespace*, page_title*, page_is_redirect*, 
                    # ... 
                    if j == 1:
                        the_rest_of_data = chunk.split(",", 1)
                        ns = the_rest_of_data[0]
                    
                        if ns == "0":
                            tmp = the_rest_of_data[1].split("',", 1)
                            page_title = tmp[0]
                            page_is_redirect = tmp[1].split(',')[0]

                            buffer += page_title + "'\n"

                            if page_is_redirect == "0":
                                if len(buffer_ns0_0redirect) >= 10_000_000:
                                    asyncio.run(write_data("../output/enwiki-20230101-page-ns0-0redirect.csv", buffer_ns0_0redirect))
                                buffer_ns0_0redirect += buffer

                            elif page_is_redirect == "1":
                                if len(buffer_ns0_1redirect) >= 10_000_000:
                                    asyncio.run(write_data("../output/enwiki-20230101-page-ns0-1redirect.csv", buffer_ns0_1redirect))
                                buffer_ns0_1redirect += buffer

                        elif ns == "14":
                            tmp = the_rest_of_data[1].split("',", 1)
                            page_title = tmp[0]
                            page_is_redirect = tmp[1].split(',')[0]

                            buffer += page_title + "'\n"

                            if page_is_redirect == "0":
                                if len(buffer_ns14_0redirect) >= 10_000_000:
                                    asyncio.run(write_data("../output/enwiki-20230101-page-ns14-0redirect.csv", buffer_ns14_0redirect))
                                buffer_ns14_0redirect += buffer

                            elif page_is_redirect == "1":
                                if len(buffer_ns14_1redirect) >= 10_000_000:
                                    asyncio.run(write_data("../output/enwiki-20230101-page-ns14-1redirect.csv", buffer_ns14_1redirect))
                                buffer_ns14_1redirect += buffer
                total_record_processed += 1
        except IndexError:
            print(record)
                
    asyncio.run(write_data("../output/enwiki-20230101-page-ns0-0redirect.csv", buffer_ns0_0redirect))
    asyncio.run(write_data("../output/enwiki-20230101-page-ns0-1redirect.csv", buffer_ns0_1redirect))
    asyncio.run(write_data("../output/enwiki-20230101-page-ns14-0redirect.csv", buffer_ns14_0redirect))
    asyncio.run(write_data("../output/enwiki-20230101-page-ns14-1redirect.csv", buffer_ns14_1redirect))
    return total_record_processed

def divide_file(filename):
    with gzip.open(filename, "r") as f:
        while True:
            lines = list(itertools.islice(f, 100))

            if not lines:
                break 

            yield lines

if __name__ == "__main__":
    start_time = time.time()
    print("Start processing...")

    with multiprocessing.Pool(16) as pool:
        total_record = 0
        for total_line in pool.imap_unordered(process_line, divide_file("../enwiki-20230101-page.sql.gz"), 1):
            total_record += total_line

    asyncio.run(write_data("enwiki-20230101-page-finish.txt", f"Total record processed: {total_record} \nProcess Duration: {time.time() - start_time}"))