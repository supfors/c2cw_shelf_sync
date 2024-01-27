#!/usr/bin/env python3

from datetime import datetime
import pandas as pd
import sqlite3
import uuid
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--log', action='store_true', help='Enable logging')
args = parser.parse_args()

current_date = datetime.now()

calibre_db = sqlite3.connect("/path/to/metadata.db")
calibre_web_db = sqlite3.connect("/path/to/app.db")

df_book_shelf_link = pd.read_sql_query(
    "SELECT book_id, shelf FROM book_shelf_link", calibre_web_db
)
df_books_tags_link = pd.read_sql_query(
    "SELECT books_tags_link.book, tags.name AS tag_name \
                                        FROM books_tags_link \
                                        JOIN tags ON books_tags_link.tag = tags.id;",
    calibre_db,
)


def get_shelves():
    return pd.read_sql_query("SELECT id, name from shelf", calibre_web_db)

def add_shelves():
    # Create a shelf for each tag in calibre database.
    df_all_shelves = get_shelves()
    df_new_shelves = pd.DataFrame(
        columns=[
            "name",
            "is_public",
            "user_id",
            "uuid",
            "created",
            "last_modified",
            "kobo_sync",
        ]
    )

    for tag_name, group_df in df_books_tags_link.groupby('tag_name'):
        if tag_name in df_all_shelves['name'].values:
            continue
        else:
            row_uuid = uuid.uuid4()
            new_row = {
                'name': tag_name,
                'is_public': int(0),
                'user_id': int(1),
                'uuid': str(row_uuid),
                'created': str(current_date),
                'last_modified': str(current_date),
                'kobo_sync': int(0)
            }
            df_new_shelves.loc[len(df_new_shelves)] = new_row

    df_new_shelves.to_sql("shelf", calibre_web_db, if_exists='append', index=False)

    if args.log:
        if df_new_shelves.empty:
            print(f'# No shelves to create\n')
        else:
            print(f"# Shelves added:\n\n{df_new_shelves[['name']].to_markdown(index=False)}\n")

def update_shelves():
    df_all_shelves = get_shelves()

    # Merge calibre and calibre-web data, rename columns to match calibre-web schema, set date_added column and remove unused columns.
    merged_df = (
        pd.merge(
            df_books_tags_link, df_all_shelves, left_on="tag_name", right_on="name"
        )
        .rename(columns={"book": "book_id", "id": "shelf"})
        .assign(date_added=current_date)
        .drop(columns=["tag_name", "name"], errors="ignore")
    )

    # Compare calibre tag/links to calibre-web shelf/links, keep rows that no longer exist in calibre.
    df_deleted_links = (
        df_book_shelf_link.merge(merged_df, how="outer", indicator=True)
        .query('_merge == "left_only"')
        .drop(columns="_merge")
    )

    # Keep missing links in calibre-web
    merged_df = (
        merged_df.merge(
            df_book_shelf_link, on=["book_id", "shelf"], how="left", indicator=True
        )
        .query('_merge == "left_only"')
        .drop(columns="_merge")
    )

    merged_df.to_sql('book_shelf_link', calibre_web_db, if_exists='append', index=False)

    # Delete obsolete records from sql.
    tuples = tuple(zip(df_deleted_links['book_id'], df_deleted_links['shelf']))
    cursor = calibre_web_db.cursor()
    sql = '''DELETE FROM book_shelf_link WHERE book_id = ? AND shelf = ?'''
    cursor.executemany(sql, tuples)

    calibre_web_db.commit()
    cursor.close()

    if args.log:
        df_books = pd.read_sql_query(
            "SELECT id, title, author_sort from books", calibre_db
        )
        df_log_add = pd.merge(merged_df, df_books, left_on="book_id", right_on="id")
        df_log_add = pd.merge(
            df_log_add, df_all_shelves, left_on="shelf", right_on="id"
        ).rename(columns={"name": "Shelf", "title": "Book", "author_sort": "Author"})

        df_log_del = pd.merge(
            df_deleted_links, df_books, left_on="book_id", right_on="id"
        )
        df_log_del = pd.merge(
            df_log_del, df_all_shelves, left_on="shelf", right_on="id"
        ).rename(columns={"name": "Shelf", "title": "Book", "author_sort": "Author"})

        if df_log_add.empty:
            print("# No links to add\n")
        else:
            print(
                f"# Links added:\n\n{df_log_add[['Shelf', 'Book', 'Author']].apply(lambda x: x.str.slice(0, 50)).sort_values(by='Shelf').to_markdown(index=False)}\n"
            )

        if df_log_del.empty:
            print("# No links to delete\n")
        else:
            print(
                f"# Links deleted:\n\n{df_log_del[['Shelf', 'Book', 'Author']].apply(lambda x: x.str.slice(0, 50)).sort_values(by='Shelf').to_markdown(index=False)}\n"
            )

def main():
    add_shelves()
    update_shelves()

    calibre_db.close()
    calibre_web_db.close()

if __name__ == "__main__":
    main()
