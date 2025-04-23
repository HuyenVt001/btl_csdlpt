import traceback
import mysql.connector

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

# SETUP Functions
def createdb(dbname):
    con = getopenconnection()
    cur = con.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {dbname}")
    con.commit()
    cur.close()
    con.close()

def delete_db(dbname):
    con = getopenconnection()
    cur = con.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS {dbname}")
    con.commit()
    cur.close()
    con.close()

def deleteAllPublicTables(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE()")
    tables = [row[0] for row in cur.fetchall()]
    for tablename in tables:
        cur.execute(f"DROP TABLE IF EXISTS {tablename}")
    openconnection.commit()
    cur.close()

def getopenconnection(user='root', password='1234', dbname=None):
    if dbname:
        return mysql.connector.connect(user=user, password=password, host='localhost', database=dbname)
    else:
        return mysql.connector.connect(user=user, password=password, host='localhost')

####### Tester support
def getCountrangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    countList = []
    interval = 5.0 / numberofpartitions
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename} WHERE rating >= 0 AND rating <= {interval}")
    countList.append(int(cur.fetchone()[0]))

    lowerbound = interval
    for i in range(1, numberofpartitions):
        cur.execute(f"SELECT COUNT(*) FROM {ratingstablename} WHERE rating > {lowerbound} AND rating <= {lowerbound + interval}")
        lowerbound += interval
        countList.append(int(cur.fetchone()[0]))

    cur.close()
    return countList

def getCountroundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    countList = []
    cur.execute(f"SELECT COUNT(*) FROM {ratingstablename}")
    total_rows = int(cur.fetchone()[0])
    for i in range(numberofpartitions):
        countList.append((total_rows + numberofpartitions - 1 - i) // numberofpartitions)
    cur.close()
    return countList

# Helpers for Tester functions
def checkpartitioncount(cursor, expectedpartitions, prefix):
    cursor.execute(f"SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE '{prefix}%'")
    count = int(cursor.fetchone()[0])
    if count != expectedpartitions:
        raise Exception(f"Expected {expectedpartitions} partition tables, found {count}")

def totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex):
    selects = []
    for i in range(partitionstartindex, n + partitionstartindex):
        selects.append(f"SELECT * FROM {rangepartitiontableprefix}{i}")
    cur.execute('SELECT COUNT(*) FROM (' + ' UNION ALL '.join(selects) + ') AS T')
    count = int(cur.fetchone()[0])
    return count

def testrangeandrobinpartitioning(n, openconnection, rangepartitiontableprefix, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    with openconnection.cursor() as cur:
        if not isinstance(n, int) or n < 0:
            checkpartitioncount(cur, 0, rangepartitiontableprefix)
        else:
            checkpartitioncount(cur, n, rangepartitiontableprefix)
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count != ACTUAL_ROWS_IN_INPUT_FILE:
                raise Exception(f"Partitioning failed. Expected {ACTUAL_ROWS_IN_INPUT_FILE} rows, got {count}")

def testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
    with openconnection.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {expectedtablename} WHERE {USER_ID_COLNAME} = %s AND {MOVIE_ID_COLNAME} = %s AND {RATING_COLNAME} = %s", (userid, itemid, rating))
        return int(cur.fetchone()[0]) == 1

def testEachRangePartition(ratingstablename, n, openconnection, rangepartitiontableprefix):
    countList = getCountrangepartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    for i in range(n):
        cur.execute(f"SELECT COUNT(*) FROM {rangepartitiontableprefix}{i}")
        count = int(cur.fetchone()[0])
        if count != countList[i]:
            raise Exception(f"{rangepartitiontableprefix}{i} has {count} rows but expected {countList[i]}")

def testEachRoundrobinPartition(ratingstablename, n, openconnection, roundrobinpartitiontableprefix):
    countList = getCountroundrobinpartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    for i in range(n):
        cur.execute(f"SELECT COUNT(*) FROM {roundrobinpartitiontableprefix}{i}")
        count = int(cur.fetchone()[0])
        if count != countList[i]:
            raise Exception(f"{roundrobinpartitiontableprefix}{i} has {count} rows but expected {countList[i]}")

# Test wrapper functions remain unchanged except for cursor usage and commits.
# You can update those in a similar fashion.

