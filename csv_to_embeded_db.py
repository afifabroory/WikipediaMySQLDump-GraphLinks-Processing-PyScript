import lmdbm as dbm
import sys
import itertools

def main():
    # Open database and create if doesn't exists with filename same as csv filename
    # with dbm.open("../database/enwiki-20230101-" + sys.argv[1] + "_id_name", "c") as db:
    with dbm.open("../database/enwiki-20230101-" + sys.argv[1] + "_name_id","c") as db2:
        MAX_CHUNK = 1_000_000
        with open("../output/enwiki-20230101-" + sys.argv[1] + ".csv", "r") as f:
            # batch_data = dict()
            batch_data2 = dict()
            cnt = 0
            for line in f:
                splitted = line.split(',', 1)
                if len(splitted) == 2:
                    batch_data[splitted[0]] = splitted[1].replace('\\\'', '\'').replace('_', ' ')[1:-2].encode("utf-8")
                    batch_data2['key:'.encode("utf-8")+splitted[1].replace('\\\'', '\'').replace('_', ' ')[1:-2].encode("utf-8")] = splitted[0]
                    cnt += 1
                else:
                    print(line, "the data source is broken.")
                if (MAX_CHUNK == cnt):
                    # db.update(batch_data)
                    db2.update(batch_data2)
                    cnt = 0
            if cnt != 0:
                # db.update(batch_data)
                db2 .update(batch_data2)

if __name__ == "__main__":
    main()