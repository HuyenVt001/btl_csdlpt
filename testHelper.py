import traceback
import pymysql

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

# SETUP Functions
def createdb(dbname):
    """
    Tạo một cơ sở dữ liệu trong MySQL.
    Kiểm tra xem cơ sở dữ liệu đã tồn tại chưa, nếu chưa thì tạo mới.
    """
    con = getopenconnection(dbname='mysql')  # Kết nối tới cơ sở dữ liệu mặc định 'mysql'
    cur = con.cursor()

    # Kiểm tra xem cơ sở dữ liệu đã tồn tại chưa
    cur.execute("SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = %s", (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % dbname)  # Tạo cơ sở dữ liệu
        con.commit()
    else:
        print('A database named "{0}" already exists'.format(dbname))

    cur.close()
    con.close()

def delete_db(dbname):
    con = getopenconnection(dbname='mysql')
    cur = con.cursor()
    cur.execute('DROP DATABASE IF EXISTS %s' % dbname)
    con.commit()
    cur.close()
    con.close()

def deleteAllPublicTables(openconnection):
    cur = openconnection.cursor()
    # Lấy danh sách tất cả bảng trong cơ sở dữ liệu hiện tại
    cur.execute("SHOW TABLES")
    tables = cur.fetchall()
    for table in tables:
        table_name = table[0]
        cur.execute("DROP TABLE IF EXISTS %s CASCADE" % table_name)
    openconnection.commit()
    cur.close()

def getopenconnection(user='root', password='291004', dbname='mysql'):
    return pymysql.connect(host='localhost', user=user, password=password, database=dbname)

# Tester support
def getCountrangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Đếm số dòng trong mỗi phân vùng range partition.
    """
    cur = openconnection.cursor()
    countList = []
    interval = 5.0 / numberofpartitions
    cur.execute("SELECT COUNT(*) FROM %s WHERE rating >= %s AND rating <= %s" % (ratingstablename, 0, interval))
    countList.append(int(cur.fetchone()[0]))

    lowerbound = interval
    for i in range(1, numberofpartitions):
        cur.execute("SELECT COUNT(*) FROM %s WHERE rating > %s AND rating <= %s" % (ratingstablename, lowerbound, lowerbound + interval))
        lowerbound += interval
        countList.append(int(cur.fetchone()[0]))

    cur.close()
    return countList

def getCountroundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Đếm số dòng trong mỗi phân vùng round-robin partition.
    """
    cur = openconnection.cursor()
    countList = []
    for i in range(0, numberofpartitions):
        cur.execute(
            "SELECT COUNT(*) FROM (SELECT *, ROW_NUMBER() OVER () AS row_num FROM %s) AS temp WHERE (row_num-1) %% %s = %s" % (
                ratingstablename, numberofpartitions, i))
        countList.append(int(cur.fetchone()[0]))

    cur.close()
    return countList

# Helpers for Tester functions
def checkpartitioncount(cursor, expectedpartitions, prefix):
    cursor.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE %s", (prefix + '%',))
    count = int(cursor.fetchone()[0])
    if count != expectedpartitions:
        raise Exception(
            'Range partitioning not done properly. Expected %s table(s) but found %s table(s)' % (
                expectedpartitions, count))

def totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex):
    selects = []
    for i in range(partitionstartindex, n + partitionstartindex):
        selects.append('SELECT * FROM %s%s' % (rangepartitiontableprefix, i))
    cur.execute('SELECT COUNT(*) FROM (%s) AS T' % ' UNION ALL '.join(selects))
    count = int(cur.fetchone()[0])
    return count

def testrangeandrobinpartitioning(n, openconnection, rangepartitiontableprefix, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    with openconnection.cursor() as cur:
        if not isinstance(n, int) or n < 0:
            checkpartitioncount(cur, 0, rangepartitiontableprefix)
        else:
            checkpartitioncount(cur, n, rangepartitiontableprefix)
            count = totalrowsinallpartitions(cur, n, rangepartitiontableprefix, partitionstartindex)
            if count < ACTUAL_ROWS_IN_INPUT_FILE:
                raise Exception(
                    "Completeness property of Partitioning failed. Expected %s rows after merging all tables, but found %s rows" % (
                        ACTUAL_ROWS_IN_INPUT_FILE, count))
            if count > ACTUAL_ROWS_IN_INPUT_FILE:
                raise Exception(
                    "Disjointness property of Partitioning failed. Expected %s rows after merging all tables, but found %s rows" % (
                        ACTUAL_ROWS_IN_INPUT_FILE, count))
            if count != ACTUAL_ROWS_IN_INPUT_FILE:
                raise Exception(
                    "Reconstruction property of Partitioning failed. Expected %s rows after merging all tables, but found %s rows" % (
                        ACTUAL_ROWS_IN_INPUT_FILE, count))

def testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
    with openconnection.cursor() as cur:
        cur.execute(
            'SELECT COUNT(*) FROM %s WHERE %s = %s AND %s = %s AND %s = %s' % (
                expectedtablename, USER_ID_COLNAME, userid, MOVIE_ID_COLNAME, itemid, RATING_COLNAME, rating))
        count = int(cur.fetchone()[0])
        if count != 1:
            return False
        return True

def testEachRangePartition(ratingstablename, n, openconnection, rangepartitiontableprefix):
    countList = getCountrangepartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    for i in range(0, n):
        cur.execute("SELECT COUNT(*) FROM %s%s" % (rangepartitiontableprefix, i))
        count = int(cur.fetchone()[0])
        if count != countList[i]:
            raise Exception("%s%s has %s rows while the correct number should be %s" % (
                rangepartitiontableprefix, i, count, countList[i]))

def testEachRoundrobinPartition(ratingstablename, n, openconnection, roundrobinpartitiontableprefix):
    countList = getCountroundrobinpartition(ratingstablename, n, openconnection)
    cur = openconnection.cursor()
    for i in range(0, n):
        cur.execute("SELECT COUNT(*) FROM %s%s" % (roundrobinpartitiontableprefix, i))
        count = cur.fetchone()[0]
        if count != countList[i]:
            raise Exception("%s%s has %s rows while the correct number should be %s" % (
                roundrobinpartitiontableprefix, i, count, countList[i]))

# Test Functions
def testloadratings(MyAssignment, ratingstablename, filepath, openconnection, rowsininpfile):
    try:
        MyAssignment.loadratings(ratingstablename, filepath, openconnection)
        with openconnection.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM %s' % ratingstablename)
            count = int(cur.fetchone()[0])
            if count != rowsininpfile:
                raise Exception(
                    'Expected %s rows, but %s rows in \'%s\' table' % (rowsininpfile, count, ratingstablename))
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

def testrangepartition(MyAssignment, ratingstablename, n, openconnection, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    try:
        MyAssignment.rangepartition(ratingstablename, n, openconnection)
        testrangeandrobinpartitioning(n, openconnection, RANGE_TABLE_PREFIX, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE)
        testEachRangePartition(ratingstablename, n, openconnection, RANGE_TABLE_PREFIX)
        return [True, None]
    except Exception as e:
        traceback.print_exc()
        return [False, e]

def testroundrobinpartition(MyAssignment, ratingstablename, numberofpartitions, openconnection,
                            partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE):
    try:
        MyAssignment.roundrobinpartition(ratingstablename, numberofpartitions, openconnection)
        testrangeandrobinpartitioning(numberofpartitions, openconnection, RROBIN_TABLE_PREFIX, partitionstartindex, ACTUAL_ROWS_IN_INPUT_FILE)
        testEachRoundrobinPartition(ratingstablename, numberofpartitions, openconnection, RROBIN_TABLE_PREFIX)
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

def testroundrobininsert(MyAssignment, ratingstablename, userid, itemid, rating, openconnection, expectedtableindex):
    try:
        expectedtablename = RROBIN_TABLE_PREFIX + expectedtableindex
        MyAssignment.roundrobininsert(ratingstablename, userid, itemid, rating, openconnection)
        if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
            raise Exception(
                'Round robin insert failed! Could not find (%s, %s, %s) tuple in %s table' % (userid, itemid, rating, expectedtablename))
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

def testrangeinsert(MyAssignment, ratingstablename, userid, itemid, rating, openconnection, expectedtableindex):
    try:
        expectedtablename = RANGE_TABLE_PREFIX + expectedtableindex
        MyAssignment.rangeinsert(ratingstablename, userid, itemid, rating, openconnection)
        if not testrangerobininsert(expectedtablename, itemid, openconnection, rating, userid):
            raise Exception(
                'Range insert failed! Could not find (%s, %s, %s) tuple in %s table' % (userid, itemid, rating, expectedtablename))
    except Exception as e:
        traceback.print_exc()
        return [False, e]
    return [True, None]

