"""
movies_queries.py
CSD-310  •  Module 7
Queries the movies database and prints four result sets.

1. All studios
2. All genres
3. Movies with run time < 120 minutes
4. Film names grouped by director

Author: <Your Name>
Date: <MM/DD/YYYY>
"""

import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error


def main():
    # ──────────────────────────────────────────────────────────────────────────
    # 1. Load credentials from .env
    #    (.env file must live in the same folder as this script)
    # ──────────────────────────────────────────────────────────────────────────
    load_dotenv()                     # makes the KEY=VALUE pairs available to os.getenv

    config = {
        "host":     os.getenv("DB_HOST", "localhost"),
        "user":     os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "database": os.getenv("DB_NAME"),
    }

    db = None
    cursor = None

    try:
        # ──────────────────────────────────────────────────────────────────────
        # 2. Connect and create cursor
        # ──────────────────────────────────────────────────────────────────────
        db = mysql.connector.connect(**config)
        cursor = db.cursor()

        # ──────────────────────────────────────────────────────────────────────
        # 3. Define and execute queries
        # ──────────────────────────────────────────────────────────────────────
        queries = [
            ("-- DISPLAYING STUDIO RECORDS --",
             "SELECT * FROM studio;"),

            ("-- DISPLAYING GENRE RECORDS --",
             "SELECT * FROM genre;"),

            ("-- DISPLAYING MOVIES SHORTER THAN 2 HOURS --",
             "SELECT film_name, film_runtime "
             "FROM film "
             "WHERE film_runtime < 120;"),

            ("-- DISPLAYING FILMS GROUPED BY DIRECTOR --",
             "SELECT film_director, "
             "       GROUP_CONCAT(film_name SEPARATOR ', ') AS movies_directed "
             "FROM film "
             "GROUP BY film_director;")
        ]

        for header, sql in queries:
            print(f"\n{header}")
            cursor.execute(sql)
            for row in cursor.fetchall():
                print(row)

    except Error as err:
        print(f"Database error: {err}")

    finally:
        # ──────────────────────────────────────────────────────────────────────
        # 4. Clean up safely, even if connection failed
        # ──────────────────────────────────────────────────────────────────────
        try:
            if cursor:
                cursor.close()
        except Exception:
            pass

        try:
            if db and db.is_connected():
                db.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
