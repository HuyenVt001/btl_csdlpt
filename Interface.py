import mysql.connector

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='root', password='291004', dbname='dds_assgn1'):
    return mysql.connector.connect(
        host='localhost',
        user=user,
        password=password,
        database=dbname
    )


def create_db(dbname):
    con = mysql.connector.connect(
        host='localhost',
        user='root',
        password='your_password'
    )
    cur = con.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {dbname};")
    cur.close()
    con.close()


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ratingstablename} (
            userid INT,
            movieid INT,
            rating FLOAT
        );
    """)

    with open(ratingsfilepath, 'r') as file:
        for line in file:
            parts = line.strip().split(':')
            if len(parts) >= 5:
                userid = int(parts[0])
                movieid = int(parts[2])
                rating = float(parts[4])
                cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, movieid, rating))

    con.commit()
    cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    delta = 5.0 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'

    for i in range(numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = f"{RANGE_TABLE_PREFIX}{i}"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (userid INT, movieid INT, rating FLOAT);")

        if i == 0:
            cur.execute(f"""
                INSERT INTO {table_name}
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE rating >= {minRange} AND rating <= {maxRange};
            """)
        else:
            cur.execute(f"""
                INSERT INTO {table_name}
                SELECT userid, movieid, rating FROM {ratingstablename}
                WHERE rating > {minRange} AND rating <= {maxRange};
            """)

    con.commit()
    cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    cur.execute(f"SELECT * FROM {ratingstablename};")
    rows = cur.fetchall()

    for i in range(numberofpartitions):
        table_name = f"{RROBIN_TABLE_PREFIX}{i}"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (userid INT, movieid INT, rating FLOAT);")

    for idx, row in enumerate(rows):
        userid, movieid, rating = row
        table_index = idx % numberofpartitions
        table_name = f"{RROBIN_TABLE_PREFIX}{table_index}"
        cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, movieid, rating))

    con.commit()
    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    cur.execute(f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename};")
    total_rows = cur.fetchone()[0]
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows - 1) % numberofpartitions
    table_name = f"{RROBIN_TABLE_PREFIX}{index}"
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))

    con.commit()
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5.0 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index -= 1
    table_name = f"{RANGE_TABLE_PREFIX}{index}"
    cur.execute(f"INSERT INTO {table_name} (userid, movieid, rating) VALUES (%s, %s, %s);", (userid, itemid, rating))

    con.commit()
    cur.close()


def count_partitions(prefix, openconnection):
    con = openconnection
    cur = con.cursor()
    cur.execute(f"SHOW TABLES LIKE '{prefix}%';")
    rows = cur.fetchall()
    cur.close()
    return len(rows)
