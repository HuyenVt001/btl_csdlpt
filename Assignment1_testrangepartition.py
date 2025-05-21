DATABASE_NAME = 'dds_assgn1'

RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
INPUT_FILE_PATH = './small_ratings.dat' #đường dẫn file đầu ra ở file data.py
ACTUAL_ROWS_IN_INPUT_FILE = 50  

import traceback
import testHelper
import Interface as MyAssignment

if __name__ == '__main__':
    try:
        # Create database
        testHelper.createdb(DATABASE_NAME)

        # Open connection using pymysql
        conn = testHelper.getopenconnection(dbname=DATABASE_NAME)

        # Delete all existing public tables
        testHelper.deleteAllPublicTables(conn)

        # Test loading ratings
        [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn, ACTUAL_ROWS_IN_INPUT_FILE)
        if result:
            print("loadratings function pass!")
        else:
            print("loadratings function fail!")

        # Test range partitioning
        [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0, ACTUAL_ROWS_IN_INPUT_FILE)
        if result:
            print("rangepartition function pass!")
        else:
            print("rangepartition function fail!")

        # ALERT:: Use only one at a time
        [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
        # [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 0, conn, '0')
        if result:
            print("rangeinsert function pass!")
        else:
            print("rangeinsert function fail!")

        # Optionally delete all tables after testing
        choice = input('Press enter to Delete all tables? ')
        if choice == '':
            testHelper.deleteAllPublicTables(conn)

        if conn.open:
            conn.close()

    except Exception as detail:
        traceback.print_exc()
