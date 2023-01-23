import lmdb
import time
import itertools
import multiprocessing
import asyncio
import aiofiles
# with lmdb.open("../database/enwiki-20230101-category_id_name") as db:
#     with db.begin() as txn:
#         with txn.cursor() as cursor:
#             with open("../output/enwiki-20230101-categorylinks-subcat.csv") as f:
#                 print(cursor.get(b'192834'))

def process_line(data):
    with lmdb.open("../database/enwiki-20230101-category_id_name") as db:
        with db.begin() as txn:
            with txn.cursor() as cursor:
                buffer = ""
                for line in data:
                    splitted = line.split(',', 1)
                    if len(splitted) == 2:
                        key = cursor.get(splitted[0].encode("utf-8"))
                        if key == None:
                            print(splitted)
                        # buffer += cursor.get(splitted[0].encode("utf-8")).decode("utf-8") + splitted[1].replace('\\\'', '\'').replace('_', ' ')[1:-1].encode("utf-8")
                        # print(splitted[0])
                        # print(cursor.get(splitted[0].encode("utf-8")).decode("utf-8") + splitted[1].replace('\\\'', '\'').replace('_', ' ')[1:-1].encode("utf-8"))
                    else:
                        print(line, "the data source is broken.")

def divide_file(filename):
    with open(filename, "r") as f:
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
        for total_line in pool.imap_unordered(process_line, divide_file("../output/enwiki-20230101-categorylinks-subcat.csv"), 1):
            pass