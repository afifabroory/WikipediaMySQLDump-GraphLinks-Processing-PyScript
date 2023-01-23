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
    buffer = ""

    total_row_processed = 0
    for line in data:
        splitted = line.decode("utf-8", errors="replace").split(" ")

        # Filter data only INSERT statement
        if splitted[0] != "INSERT":
            continue

        # Split by '),('. The only reliable way to extract the data
        data = splitted[-1].split("),(")

        try:
            for i, record in enumerate(data):
                for j, chunk in enumerate(record.split(",", 2)):
                    # cat_id
                    if j == 0:
                        # If chunk are first in line.
                        if i == 0:
                            buffer += chunk[1:] + ","
                        else:
                            buffer += chunk + ","
                        continue
                    
                    # cat_namespace
                    if j == 1:
                        buffer += chunk + "\n"
                        continue

                    # cat_pages, cat_subcats, cat_files, cat_hidden
                    if j >= 2:
                        continue

                    # Write the data per 10 MB (approx.); 1 character = 1 byte. 10 milion character = 10 MB
                    if len(buffer) >= 10_000_000:
                        asyncio.run(write_data("../output/enwiki-20230101-category.csv", buffer))
                        
                    buffer = ""
                total_row_processed += 1
        except IndexError:
            print(record)
                
    asyncio.run(write_data("../output/enwiki-20230101-category.csv", buffer))
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
        for total_line in pool.imap_unordered(process_line, divide_file("../enwiki-20230101-category.sql.gz"), 1):
            total_line_processed += total_line

    asyncio.run(write_data("enwiki-20230101-category-finish.txt", f"Total line processed: {total_line_processed} \nProcess Duration: {time.time() - start_time}"))