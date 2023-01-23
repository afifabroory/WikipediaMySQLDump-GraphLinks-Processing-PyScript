# Code optimized for pagelinks

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
    RELATION_NS0_TO_NS0 = ""
    RELATION_NS14_TO_NS14 = ""
    RELATION_NS0_TO_NS14 = ""
    RELATION_NS14_TO_NS0 = ""

    for line in data:
        splitted = line.decode("utf-8", errors="replace").split(" ")

        # Filter data only INSERT statement
        if splitted[0] != "INSERT":
            continue

        # Split by '),('. The only reliable way to extract the data
        data = splitted[-1].split("),(")
        ns0_from = ns0_dest = False
        ns14_from = ns14_dest = False
        relation = ""

        try:
            for j, record in enumerate(data):
                for i, chunk in enumerate(record.split(",", 2)):
                    # pl_from
                    if i%4 == 0:

                        # If chunk are first in line.
                        if j == 0:
                            relation += chunk[1:] + ","
                        else:
                            relation += chunk + ","
                        continue
                    
                    # pl_namespace
                    if i%4 == 1:
                        ns0_dest = (chunk == "0")
                        ns14_dest = (chunk == "14")
                        continue

                    # pl_title and pl_namespace_from
                    if i%4 == 2:
                        chunk = chunk.rsplit(",", 1)
                        relation += chunk[0][1:-1].replace("\\", "")
                        ns0_from = (chunk[1] == "0")
                        ns14_from = (chunk[1] == "14")

                    # Write the data per 10 MB (approx.); 1 character = 1 byte. 10 milion character = 10 MB
                    if ns0_dest == ns0_from and ns0_dest == True:
                        if len(RELATION_NS0_TO_NS0) >= 10_000_000:
                            asyncio.run(write_data("../output/enwiki-20230101-pagelinks-ns0-to-ns0.csv", RELATION_NS0_TO_NS0))
                            RELATION_NS0_TO_NS0 = ""
                        else:
                            RELATION_NS0_TO_NS0 += f"{relation}\n"
                        ns0_from = ns0_dest = False
                        ns14_from = ns14_dest = False

                    if ns14_dest == ns14_from and ns14_dest == True:
                        if len(RELATION_NS14_TO_NS14) >= 10_000_000:
                            asyncio.run(write_data("../output/enwiki-20230101-pagelinks-ns14-to-ns14.csv", RELATION_NS14_TO_NS14))
                            RELATION_NS14_TO_NS14 = ""
                        else:
                            RELATION_NS14_TO_NS14 += f"{relation}\n"
                        ns0_from = ns0_dest = False
                        ns14_from = ns14_dest = False
                    
                    if ns0_dest == ns14_from and ns14_dest == True:
                        if len(RELATION_NS0_TO_NS14) >= 10_000_000:
                            asyncio.run(write_data("../output/enwiki-20230101-pagelinks-ns0-to-ns14.csv", RELATION_NS0_TO_NS14))
                            RELATION_NS0_TO_NS14 = ""
                        else:
                            RELATION_NS0_TO_NS14 += f"{relation}\n"
                        ns0_from = ns0_dest = False
                        ns14_from = ns14_dest = False

                    if ns14_dest == ns0_from and ns0_dest == True:
                        if len(RELATION_NS14_TO_NS0) >= 10_000_000:
                            asyncio.run(write_data("../output/enwiki-20230101-pagelinks-ns14-to-ns0.csv", RELATION_NS14_TO_NS0))
                            RELATION_NS14_TO_NS0 = ""
                        else:
                            RELATION_NS14_TO_NS0 += f"{relation}\n"
                        ns0_from = ns0_dest = False
                        ns14_from = ns14_dest = False
                    relation = ""
        except IndexError:
            print(record)
                
    asyncio.run(write_data("../output/enwiki-20230101-pagelinks-ns0-to-ns0.csv", RELATION_NS0_TO_NS0))
    asyncio.run(write_data("../output/enwiki-20230101-pagelinks-ns14-to-ns14.csv", RELATION_NS14_TO_NS14))
    asyncio.run(write_data("../output/enwiki-20230101-pagelinks-ns0-to-ns14.csv", RELATION_NS0_TO_NS14))
    asyncio.run(write_data("../output/enwiki-20230101-pagelinks-ns14-to-ns0.csv", RELATION_NS14_TO_NS0))

    return len(data)

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
        for total_line in pool.imap_unordered(process_line, divide_file("../enwiki-20230101-pagelinks.sql.gz"), 1):
            total_line_processed += total_line

    asyncio.run(write_data("../output/enwiki-20230101-pagelinks-finish.txt", f"Total line processed: {total_line_processed} \nProcess Duration: {time.time() - start_time}"))