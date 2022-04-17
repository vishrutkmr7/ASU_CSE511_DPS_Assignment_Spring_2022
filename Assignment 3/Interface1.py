#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import math


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
    database_cursor = openconnection.cursor()
    database_cursor.execute(
        "Create TABLE "
        + ratingstablename
        + " (userid INT,temp1 VARCHAR, movieid INT,temp2 VARCHAR,rating FLOAT,temp3 VARCHAR,timestamp BIGINT)"
    )
    openconnection.commit()
    file = open(ratingsfilepath, "r")
    database_cursor.copy_from(
        file,
        ratingstablename,
        sep=":",
        columns=("userid", "temp1", "movieid", "temp2", "rating", "temp3", "timestamp"),
    )
    openconnection.commit()
    database_cursor.execute(
        "ALTER TABLE "
        + ratingstablename
        + " DROP COLUMN temp1, DROP COLUMN temp2,DROP COLUMN temp3, DROP COLUMN timestamp"
    )
    database_cursor.close()
    openconnection.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        return
    if math.floor(numberofpartitions) != math.ceil(numberofpartitions):
        return
    name = "range_part"
    values = round((5 / numberofpartitions), 2)
    border = 0
    list_of_partitions = [name + str(k) for k in range(numberofpartitions)]
    for k in range(numberofpartitions):
        database_cursor = openconnection.cursor()
        database_cursor.execute(
            "Create TABLE "
            + list_of_partitions[k]
            + " (userid INT,movieid INT,rating FLOAT)"
        )
        openconnection.commit()
        if k == 0:
            database_cursor.execute(
                "insert into "
                + list_of_partitions[k]
                + " (userid, movieid, rating) select userid, movieid, rating from "
                + ratingstablename
                + " where rating >= "
                + str(0)
                + " and rating <= "
                + str(values)
                + ";"
            )
            openconnection.commit()
            border = values
        else:
            database_cursor.execute(
                "insert into "
                + list_of_partitions[k]
                + " (userid, movieid, rating) select userid, movieid, rating from "
                + ratingstablename
                + " where rating > "
                + str(border)
                + " and rating <= "
                + str(border + values)
                + ";"
            )
            openconnection.commit()
            border = border + values


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    if numberofpartitions <= 0:
        return
    if math.floor(numberofpartitions) != math.ceil(numberofpartitions):
        return
    name = "rrobin_part"
    list_of_partitions = [name + str(k) for k in range(numberofpartitions)]
    for k in range(numberofpartitions):
        database_cursor = openconnection.cursor()
        database_cursor.execute(
            "Create TABLE "
            + list_of_partitions[k]
            + " (userid INT,movieid INT,rating FLOAT)"
        )
        openconnection.commit()
        if k != numberofpartitions - 1:
            database_cursor.execute(
                "insert into "
                + list_of_partitions[k]
                + " select userid,movieid,rating from (select row_number() over() as row_id, * from "
                + ratingstablename
                + ") as imp where row_id%"
                + str(numberofpartitions)
                + "="
                + str(k + 1)
            )
        else:
            database_cursor.execute(
                "insert into "
                + list_of_partitions[k]
                + " select userid,movieid,rating from (select row_number() over() as row_id, * from "
                + ratingstablename
                + ") as imp where row_id%"
                + str(numberofpartitions)
                + "="
                + str(0)
            )

        openconnection.commit()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    if rating < 0 or rating > 5:
        return
    database_cursor = openconnection.cursor()
    database_cursor.execute(
        "select count(*) from (SELECT tablename FROM pg_catalog.pg_tables WHERE tablename like 'rrobin_part%') as temp"
    )
    count_of_partition = int(database_cursor.fetchone()[0])
    database_cursor.execute(f"SELECT COUNT(*) from {ratingstablename}")
    count_of_dataset = int(database_cursor.fetchone()[0])
    table_name = f"rrobin_part{str(count_of_dataset % count_of_partition)}"
    database_cursor.execute(
        "insert into "
        + ratingstablename
        + " (userid,movieid,rating) values ("
        + str(userid)
        + ","
        + str(itemid)
        + ","
        + str(rating)
        + ")"
    )
    openconnection.commit()
    database_cursor.execute(
        "insert into "
        + table_name
        + " (userid,movieid,rating) values ("
        + str(userid)
        + ","
        + str(itemid)
        + ","
        + str(rating)
        + ")"
    )
    openconnection.commit()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    if rating < 0 or rating > 5:
        return
    database_cursor = openconnection.cursor()
    database_cursor.execute(
        "select count(*) from (SELECT tablename FROM pg_catalog.pg_tables WHERE tablename like 'range_part%') as temp"
    )
    count_of_partition = int(database_cursor.fetchone()[0])
    values = round((5 / count_of_partition), 2)
    no_of_partitions = int(rating / values)
    if rating % values == 0 and no_of_partitions != 0:
        no_of_partitions -= 1
    table_name = f"range_part{no_of_partitions}"
    database_cursor.execute(
        "insert into "
        + ratingstablename
        + " (userid,movieid,rating) values ("
        + str(userid)
        + ","
        + str(itemid)
        + ","
        + str(rating)
        + ")"
    )
    openconnection.commit()
    database_cursor.execute(
        "insert into "
        + table_name
        + " (userid,movieid,rating) values ("
        + str(userid)
        + ","
        + str(itemid)
        + ","
        + str(rating)
        + ")"
    )
    openconnection.commit()


def createDB(dbname="dds_assignment"):
    connec = getOpenConnection(dbname="postgres")
    connec.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    curr = connec.cursor()
    curr.execute(
        "SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='%s'" % (dbname,)
    )
    counts = curr.fetchone()[0]
    if counts == 0:
        curr.execute(f"CREATE DATABASE {dbname}")
    else:
        print("A database named {0} already exists".format(dbname))
    curr.close()
    connec.close()


def deletepartitionsandexit(openconnection):
    curr = openconnection.cursor()
    curr.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    )
    k = [i[0] for i in curr]
    for nameoftable in k:
        curr.execute("drop table if exists {0} CASCADE".format(nameoftable))
    curr.close()


def deleteTables(ratingstablename, openconnection):
    try:
        database_cursor = openconnection.cursor()
        if ratingstablename.upper() == "ALL":
            database_cursor.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            )
            all_tables = database_cursor.fetchall()
            for table in all_tables:
                database_cursor.execute(f"DROP TABLE {table[0]} CASCADE")
        else:
            database_cursor.execute(f"DROP TABLE {ratingstablename} CASCADE")
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print(f"Error {e}")
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print(f"Error {e}")
    finally:
        if database_cursor:
            database_cursor.close()
