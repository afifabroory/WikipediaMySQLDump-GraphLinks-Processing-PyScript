import time
import itertools
import multiprocessing

def process_line(data):
    for line in data:
        col = line.split(",", 1)
        # print(line.split(',', 1)[1], end='')
        # print(line + '\n' if len(col) < 2 else '', end='')
        if col[1] == "'Main_topic_classifications'\n":
            print(line)

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
        for total_line in pool.imap_unordered(process_line, divide_file("../output/enwiki-20230101-category.csv"), 1):
            pass