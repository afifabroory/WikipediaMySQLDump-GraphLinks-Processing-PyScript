# Code optimized for categorylinks

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
    buffer_subcat = ""
    buffer_page = ""

    total_row_processed = 0
    for line in data:
        splitted = line.decode("utf-8", errors="replace").split(" ", 4)

        # Filter data only INSERT statement
        if splitted[0] != "INSERT":
            continue

        # Split by '),('. The only reliable way to extract the data
        data = splitted[-1].split("),(")

        try:
            for i, record in enumerate(data):
                buffer = ""
                for j, chunk in enumerate(record.split(",", 2)):
                    # cl_from
                    if j == 0:
                        # If chunk are first in line.
                        if i == 0:
                            buffer += chunk[1:] + ","
                        else:
                            buffer += chunk + ","
                        continue
                    
                    # cl_to
                    if j == 1:
                        buffer += chunk + "\n"
                        continue

                    # cl_sortkey, cl_timestamp, cl_sortkey_prefix, cl_collation, cl_type
                    if j >= 2:
                        cl_type = chunk.rsplit(",", 1)[-1][1:-1].replace(");", "").replace("'", "")

                        # Write the data per 10 MB (approx.); 1 character = 1 byte. 10 milion character = 10 MB
                        if cl_type == "file":
                            continue
                        if cl_type == "subcat":
                            if len(buffer) >= 10_000_000:
                                asyncio.run(write_data("../output/enwiki-20230101-categorylinks-subcat.csv", buffer_subcat))
                            buffer_subcat += buffer
                        elif cl_type == "page":
                            if len(buffer) >= 10_000_000:
                                asyncio.run(write_data("../output/enwiki-20230101-categorylinks-page.csv", buffer_page))
                            buffer_page += buffer
                        else:
                            print(cl_type)

        except IndexError:
            print(record)
                
    asyncio.run(write_data("../output/enwiki-20230101-categorylinks-subcat.csv", buffer_subcat))
    asyncio.run(write_data("../output/enwiki-20230101-categorylinks-page.csv", buffer_page))
    return total_row_processed

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
        total_line_processed = 0
        for total_line in pool.imap_unordered(process_line, divide_file("../enwiki-20230101-categorylinks.sql.gz"), 1):
            total_line_processed += total_line

    asyncio.run(write_data("enwiki-20230101-categorylinks-finish.txt", f"Total line processed: {total_line_processed} \nProcess Duration: {time.time() - start_time}"))