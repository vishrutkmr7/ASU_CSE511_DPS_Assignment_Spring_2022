#!/usr/bin/python2.7
#
# Interface for the assignment
#

import psycopg2


def getOpenConnection(user="postgres", password="1234", dbname="postgres"):
    return psycopg2.connect(
        "dbname='"
        + dbname
        + "' user='"
        + user
        + "' host='localhost' password='"
        + password
        + "'"
    )


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("drop table if exists {}".format(ratingstablename))
    conn.commit()
    cur.execute(
        "CREATE TABLE {} (userid int,test1 varchar(15),movieid int,test2 varchar(15),rating float,test3 varchar(15),time_stamp int);".format(
            ratingstablename
        )
    )
    file = open(ratingsfilepath, "r")
    cur.copy_from(
        file,
        ratingstablename,
        sep=":",
        columns=(
            "userid",
            "test1",
            "movieid",
            "test2",
            "rating",
            "test3",
            "time_stamp",
        ),
    )
    cur.execute(
        "ALTER TABLE {} drop column test1,drop column test2,drop column test3,drop column time_stamp".format(
            ratingstablename
        )
    )
    file.close()
    conn.commit()
    cur.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    if not isinstance(numberofpartitions, int) or numberofpartitions < 0:
        return
    conn = openconnection
    cur = conn.cursor()
    part = 5 / numberofpartitions
    start = 0
    for tableno in range(numberofpartitions):
        endr = start + part
        if tableno == 0:
            cur.execute(
                "SELECT * FROM {} WHERE {}.Rating >= {} AND {}.Rating <= {} ORDER BY Rating ASC".format(
                    ratingstablename, ratingstablename, start, ratingstablename, endr
                )
            )
        else:
            cur.execute(
                "SELECT * FROM {} WHERE {}.Rating > {} AND {}.Rating <= {} ORDER BY Rating ASC".format(
                    ratingstablename, ratingstablename, start, ratingstablename, endr
                )
            )

        start = endr
        temp = cur.fetchall()
        cur.execute("DROP TABLE IF EXISTS range_part{}".format(tableno))
        cur.execute(
            """
            CREATE TABLE range_part{} (
                UserID int,
                MovieID int,
                Rating float,
                PRIMARY KEY (UserID, MovieID)
            )
            """.format(
                tableno
            )
        )
        for r in temp:
            cur.execute(
                "INSERT INTO range_part{}(UserID, MovieID, Rating) VALUES ({},{},{})".format(
                    tableno, r[0], r[1], r[2]
                )
            )


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    if not isinstance(numberofpartitions, int) or numberofpartitions < 0:
        return
    conn = openconnection
    cur = conn.cursor()
    for i in range(numberofpartitions):
        cur.execute("DROP TABLE IF EXISTS rrobin_part{}".format(i))
        cur.execute(
            """
            CREATE TABLE rrobin_part{} (
                UserID int,
                MovieID int,
                Rating float,
                PRIMARY KEY (UserID, MovieID)
            )
            """.format(
                i
            )
        )

    cur.execute("SELECT * FROM {}".format(ratingstablename))
    temp = cur.fetchall()
    for rn, rd in enumerate(temp):
        i = rn % numberofpartitions
        cur.execute(
            "INSERT INTO rrobin_part{}(UserID, MovieID, Rating) VALUES ({},{},{})".format(
                i, rd[0], rd[1], rd[2]
            )
        )


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name LIKE 'rrobin_part%' AND table_schema not in ('information_schema', 'pg_catalog')
        and table_type = 'BASE TABLE'
        """
    )
    n = int(cur.fetchall()[0][0])
    cur.execute(
        """
            SELECT COUNT(*)
            FROM rrobin_part0
            """
    )
    rowcount = int(cur.fetchall()[0][0])
    tableno = 0
    for i in range(1, n):
        cur.execute(
            """
            SELECT COUNT(*)
            FROM rrobin_part{}
            """.format(
                i
            )
        )
        num_rows = int(cur.fetchall()[0][0])
        if num_rows < rowcount:
            rowcount = num_rows
            tableno = i

    cur.execute(
        "INSERT INTO rrobin_part{}(UserID, MovieID, Rating) VALUES ({},{},{})".format(
            tableno, userid, itemid, rating
        )
    )
    cur.execute(
        "INSERT INTO {} (UserID, MovieID, Rating) VALUES ({},{},{})".format(
            ratingstablename, userid, itemid, rating
        )
    )


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(table_name)
        FROM information_schema.tables
        WHERE table_name LIKE 'range_part%' AND table_schema not in ('information_schema', 'pg_catalog')
        and table_type = 'BASE TABLE'
        """
    )

    np = int(cur.fetchall()[0][0])
    part = 5 / np
    tableno = rating / part
    if rating % part == 0 and tableno != 0:
        tableno -= 1

    cur.execute(
        "INSERT INTO range_part{}(UserID, MovieID, Rating) VALUES ({},{},{})".format(
            tableno, userid, itemid, rating
        )
    )


def createDB(dbname="dds_assignment"):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname="postgres")
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute(
        "SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='%s'" % (dbname,)
    )
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute("CREATE DATABASE %s" % (dbname,))  # Create the database
    else:
        print("A database named {0} already exists".format(dbname))

    # Clean up
    cur.close()
    con.close()


def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    )
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()


def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == "ALL":
            cursor.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute("DROP TABLE %s CASCADE" % (table_name[0]))
        else:
            cursor.execute("DROP TABLE %s CASCADE" % (ratingstablename))
        openconnection.commit()
    except (psycopg2.DatabaseError) as e:
        if openconnection:
            openconnection.rollback()
        print("Error %s" % e)
    except (IOError) as e:
        if openconnection:
            openconnection.rollback()
        print("Error %s" % e)
    finally:
        if cursor:
            cursor.close()
