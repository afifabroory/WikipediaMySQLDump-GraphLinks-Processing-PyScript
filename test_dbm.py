import lmdb

with lmdb.open("../database/enwiki-20230101-category_name_id") as db:
    with db.begin() as txn:
        with txn.cursor() as cursor:
            print(cursor.get(b'Client state'))