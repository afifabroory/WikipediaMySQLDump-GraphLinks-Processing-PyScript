import gzip
import time
import itertools
import multiprocessing
import concurrent.futures

def process_line(data):
    for line in data:
        splitted = line.decode("utf-8", errors="replace").split(" ")

        # Filter data only INSERT statement
        if splitted[0] != "INSERT":
            continue

        # Clean the data
        actual_record = splitted[-1].replace("),", " ") \
                                  .replace("(", "") \
                                  .replace(");", "") \
                                  .split(" ")

        for record in actual_record:
            field = record.split(",")

    return len(data)

def divide_file(filename):
    with gzip.open(filename, "r") as f:
        while True:
            lines = list(itertools.islice(f, 1000))

            if not lines:
                break 

            yield lines

if __name__ == "__main__":
    start_time = time.time()
    print("Start processing...")


    total_line_processed = 0
    with gzip.open("../enwiki-20230101-page.sql.gz", "r") as f:
        for line in f:
            pass
            total_line_processed += 1

    # with multiprocessing.Pool() as pool:
    #     total_line_processed = 0
    #     for total_line in pool.imap_unordered(process_line, divide_file("enwiki-20230101-page.sql.gz")):
    #         total_line_processed += total_line
            # print(total_line_processed)

    # with concurrent.futures.ProcessPoolExecutor() as executor:
    #     total_line_processed = 0
    #     for total_line in executor.map(process_line, divide_file("enwiki-20230101-page.sql.gz")):
    #         total_line_processed += total_line
    #         # print(total_line_processed)

    print(total_line_processed)
    print(f"Process Duration: {time.time() - start_time}")