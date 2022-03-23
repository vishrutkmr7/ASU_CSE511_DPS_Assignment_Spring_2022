# !/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

# Do not close the connection inside this file i.e. do not perform openconnection.close()
name_of_database = "dds_assignment"
prefix_of_range_table = "RangeRatingsPart"
prefix_round_robin_table = "RoundRobinRatingsPart"


def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    cursors = openconnection.cursor()
    l_new = []

    cursors.execute("select count(*) from RangeRatingsMetaData;")
    countrange = int(cursors.fetchone()[0])

    for i in range(countrange):
        l_new.append(
            "SELECT '"
            + prefix_of_range_table
            + str(i)
            + "' AS tablename, userid, movieid, rating FROM rangeratingspart"
            + str(i)
            + " WHERE rating >= "
            + str(ratingMinValue)
            + " AND rating <= "
            + str(ratingMaxValue)
        )

    cursors.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata")
    roundpartitions = int(cursors.fetchone()[0])

    for i in range(roundpartitions):
        l_new.append(
            "SELECT '"
            + prefix_round_robin_table
            + str(i)
            + "' AS tablename, userid, movieid, rating FROM roundrobinratingspart"
            + str(i)
            + " WHERE rating >= "
            + str(ratingMinValue)
            + " AND rating <= "
            + str(ratingMaxValue)
        )

    queryop = "SELECT * FROM ({0}) AS T".format(" UNION ALL ".join(l_new))
    fileop = open("RangeQueryOut.txt", "w")

    write_file = (
        "COPY ("
        + queryop
        + ") TO '"
        + os.path.abspath(fileop.name)
        + "' (FORMAT text, DELIMITER ',')"
    )
    cursors.execute(write_file)

    cursors.close()
    fileop.close()


def PointQuery(ratingsTableName, ratingValue, openconnection):
    cursors = openconnection.cursor()
    l_new = []

    cursors.execute("SELECT COUNT(*) FROM RangeRatingsMetadata")
    countrange = int(cursors.fetchone()[0])

    for i in range(countrange):
        l_new.append(
            "SELECT '"
            + prefix_of_range_table
            + str(i)
            + "'AS tablename, userid, movieid, rating FROM rangeratingspart"
            + str(i)
            + " WHERE rating = "
            + str(ratingValue)
        )

    cursors.execute("SELECT PartitionNum FROM RoundRobinRatingsMetadata")
    roundnpartitions = int(cursors.fetchone()[0])

    for i in range(roundnpartitions):
        l_new.append(
            "SELECT '"
            + prefix_round_robin_table
            + str(i)
            + "' AS tablename, userid, movieid, rating FROM roundrobinratingspart"
            + str(i)
            + " WHERE rating = "
            + str(ratingValue)
        )

    queryop = "SELECT * FROM ({0}) AS T".format(" UNION ALL ".join(l_new))
    fileop = open("PointQueryOut.txt", "w")

    filewrite = (
        "COPY ("
        + queryop
        + ") TO '"
        + os.path.abspath(fileop.name)
        + "' (FORMAT text, DELIMITER ',')"
    )

    cursors.execute(filewrite)

    cursors.close()
    fileop.close()


def writeToFile(filename, rows):
    f = open(filename, "w")
    for line in rows:
        f.write(",".join(str(s) for s in line))
        f.write("\n")
    f.close()
