"""
movies_update_and_delete.py
CSD-310 • Module 8
Demonstrates INSERT, UPDATE, and DELETE with the movies database.

Author: Reed Bunnell
Date: <MM/DD/YYYY>
"""

import os
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import Error


def show_films(cursor, title):
    print(f"\n-- {title} --")
    query = (
        "SELECT film_name AS Name, "
        "film_director AS Director, "
        "genre_name AS Genre, "
        "studio_name AS Studio "
        "FROM film "
        "INNER JOIN genre ON film.genre_id = genre.genre_id "
        "INNER JOIN studio ON film.studio_id = studio.studio_id;"
    )
    cursor.execute(query)
    results = cursor.fetchall()
    for name, director, genre, studio in results:
        print(f"Name: {name}\nDirector: {director}\nGenre: {genre}\nStudio: {studio}\n")


def main():
    load_dotenv()

    config = {
        "host": os.getenv("DB_HOST", "localhost"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "database": os.getenv("DB_NAME", "movies")
    }

    db = None
    cursor = None

    try:
        db = mysql.connector.connect(**config)
        cursor = db.cursor()

        # 1. Display all films
        show_films(cursor, "DISPLAYING FILMS")

        # 2. Get any valid studio_id
        cursor.execute("SELECT studio_id FROM studio LIMIT 1;")
        studio_id = cursor.fetchone()[0]

        # 3. Get genre_id for 'Sci-Fi' or fallback to first genre
        cursor.execute("SELECT genre_id FROM genre WHERE genre_name = 'Sci-Fi' LIMIT 1;")
        row = cursor.fetchone()
        if row:
            scifi_id = row[0]
        else:
            cursor.execute("SELECT genre_id FROM genre LIMIT 1;")
            scifi_id = cursor.fetchone()[0]

        # 4. Insert 'Interstellar'
        insert_query = (
            "INSERT INTO film (film_name, film_releaseDate, film_runtime, "
            "film_director, studio_id, genre_id) "
            "VALUES ('Interstellar', '2014', 169, 'Christopher Nolan', %s, %s);"
        )
        cursor.execute(insert_query, (studio_id, scifi_id))
        db.commit()
        show_films(cursor, "DISPLAYING FILMS AFTER INSERT")

        # 5. Get or create 'Horror' genre
        cursor.execute("SELECT genre_id FROM genre WHERE genre_name = 'Horror' LIMIT 1;")
        row = cursor.fetchone()
        if row:
            horror_id = row[0]
        else:
            cursor.execute("INSERT INTO genre (genre_name) VALUES ('Horror');")
            horror_id = cursor.lastrowid
            db.commit()

        # 6. Update 'Alien' to genre 'Horror'
        cursor.execute("UPDATE film SET genre_id = %s WHERE film_name = 'Alien';", (horror_id,))
        db.commit()
        show_films(cursor, "DISPLAYING FILMS AFTER UPDATE")

        # 7. Delete 'Gladiator'
        cursor.execute("DELETE FROM film WHERE film_name = 'Gladiator';")
        db.commit()
        show_films(cursor, "DISPLAYING FILMS AFTER DELETE")

    except Error as err:
        print(f"Database error: {err}")

    finally:
        if cursor:
            cursor.close()
        if db and db.is_connected():
            db.close()


if __name__ == "__main__":
    main()
