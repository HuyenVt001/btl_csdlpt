import pymysql

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='root', password='291004', dbname='mysql'):
    return pymysql.connect(host='localhost', user=user, password=password, database=dbname)


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Function to load data in @ratingsfilepath file to a table called @ratingstablename.
    """
    delete_db(DATABASE_NAME)
    create_db(DATABASE_NAME)
    con = openconnection
    cur = con.cursor()
    # Tạo bảng với các cột tạm thời để load dữ liệu
    cur.execute(
        "CREATE TABLE %s (userid INTEGER, movieid INTEGER, rating FLOAT, timestamp BIGINT)" % ratingstablename)

    insert_query = f"INSERT INTO {ratingstablename} (userid, movieid, rating, timestamp) VALUES (%s, %s, %s, %s)"
    
    with open(ratingsfilepath, 'r') as f:
        data = []
        for line in f:
            values = line.strip().split('::')
            if len(values) != 4:
                print(f"Skipping invalid line: {line.strip()} (expected 4 values, got {len(values)})")
                continue
            try:
                userid = int(values[0])
                movieid = int(values[1])
                rating = float(values[2])
                timestamp = int(values[3])
                data.append((userid, movieid, rating, timestamp))
            except ValueError as e:
                print(f"Skipping invalid line: {line.strip()} (error: {e})")
                continue
        if not data:
            raise ValueError("No valid data found in the file.")
        cur.executemany(insert_query, data)

    cur.execute(
        "ALTER TABLE %s DROP COLUMN timestamp" % ratingstablename)

    con.commit()
    cur.close()


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table based on range of ratings.
    """
    con = openconnection
    cur = con.cursor()
    delta = 5.0 / numberofpartitions
    RANGE_TABLE_PREFIX = 'range_part'

    for i in range(0, numberofpartitions):
        minRange = i * delta
        maxRange = minRange + delta
        table_name = RANGE_TABLE_PREFIX + str(i)
        cur.execute("CREATE TABLE %s (userid INTEGER, movieid INTEGER, rating FLOAT)" % table_name)
        if i == 0:
            cur.execute(
                "INSERT INTO %s (userid, movieid, rating) SELECT userid, movieid, rating FROM %s WHERE rating >= %s AND rating <= %s" % (
                    table_name, ratingstablename, minRange, maxRange))
        else:
            cur.execute(
                "INSERT INTO %s (userid, movieid, rating) SELECT userid, movieid, rating FROM %s WHERE rating > %s AND rating <= %s" % (
                    table_name, ratingstablename, minRange, maxRange))

    con.commit()
    cur.close()


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Function to create partitions of main table using round robin approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    for i in range(0, numberofpartitions):
        table_name = RROBIN_TABLE_PREFIX + str(i)
        cur.execute("CREATE TABLE %s (userid INTEGER, movieid INTEGER, rating FLOAT)" % table_name)
        cur.execute(
            "INSERT INTO %s (userid, movieid, rating) SELECT userid, movieid, rating FROM "
            "(SELECT userid, movieid, rating, ROW_NUMBER() OVER () AS rnum FROM %s) AS temp "
            "WHERE (rnum-1) %% %s = %s" % (table_name, ratingstablename, numberofpartitions, i)
        )

    con.commit()
    cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on round robin approach.
    """
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    # Chèn vào bảng chính
    cur.execute(
        "INSERT INTO %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (ratingstablename, userid, itemid, rating))

    # Đếm số dòng trong bảng chính để tính partition
    cur.execute("SELECT COUNT(*) FROM %s" % ratingstablename)
    total_rows = cur.fetchone()[0]

    # Đếm số partition
    numberofpartitions = count_partitions(RROBIN_TABLE_PREFIX, openconnection)
    index = (total_rows - 1) % numberofpartitions
    table_name = RROBIN_TABLE_PREFIX + str(index)

    # Chèn vào partition tương ứng
    cur.execute("INSERT INTO %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (table_name, userid, itemid, rating))

    con.commit()
    cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Function to insert a new row into the main table and specific partition based on range rating.
    """
    con = openconnection
    cur = con.cursor()
    RANGE_TABLE_PREFIX = 'range_part'

    # Đếm số partition
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection)
    delta = 5.0 / numberofpartitions
    index = int(rating / delta)
    if rating % delta == 0 and index != 0:
        index = index - 1
    table_name = RANGE_TABLE_PREFIX + str(index)

    # Chèn vào bảng chính
    cur.execute(
        "INSERT INTO %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (ratingstablename, userid, itemid, rating))

    # Chèn vào partition tương ứng
    cur.execute("INSERT INTO %s (userid, movieid, rating) VALUES (%s, %s, %s)" % (table_name, userid, itemid, rating))

    con.commit()
    cur.close()


def create_db(dbname):
    """
    Tạo cơ sở dữ liệu nếu chưa tồn tại.
    """
    con = getopenconnection(dbname='mysql')
    cur = con.cursor()

    # Kiểm tra xem cơ sở dữ liệu đã tồn tại chưa
    cur.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = %s", (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % dbname)
        con.commit()
    else:
        print('A database named {0} already exists'.format(dbname))

    cur.close()
    con.close()

def delete_db(dbname):
    con = getopenconnection(user='root', password='291004', dbname=None)
    cur = con.cursor()
    cur.execute('DROP DATABASE IF EXISTS %s' % dbname)
    con.commit()
    cur.close()
    con.close()


def count_partitions(prefix, openconnection):
    """
    Đếm số bảng có tiền tố @prefix trong tên.
    """
    con = openconnection
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE %s",
                (prefix + '%',))
    count = cur.fetchone()[0]
    cur.close()
    return count