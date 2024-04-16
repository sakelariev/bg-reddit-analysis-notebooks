"""Download .zst files of interest from https://the-eye.eu/redarcs/"""
import pandas as pd
import zstandard as zstd
import pathlib
from sqlalchemy import create_engine
import click


def zst_to_df(zst_file: pathlib.Path):
    """Load up a zst file and convert it to a pandas DataFrame."""
    print(f"Reading {zst_file}...")
    with open(zst_file, "rb") as compressed_file:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(compressed_file) as reader:
            decompressed_data = reader.read().decode("utf-8")

    lines = decompressed_data.strip().split("\n")
    print(f"Converting {len(lines)} lines to DataFrame...")
    data = [pd.read_json(line, typ="series") for line in lines]
    df = pd.DataFrame(data)
    return df


def clean_up_submissions_df(df):
    """Clean up the submissions DataFrame."""
    selected_columns = [
        "author",
        "title",
        "selftext",
        "name",
        "id",
        "likes",
        "upvote_ratio",
        "url",
        "view_count",
        "created",
        "created_utc",
    ]
    df = df[selected_columns]
    # Drop rows where 'selftext' is '[deleted]'
    df = df.loc[df["selftext"] != "[deleted]"]
    return df


def clean_up_comments(df):
    """Clean up the comments DataFrame."""
    selected_columns = [
        "author",
        "body",
        "comment_type",
        "score",
        "replies",
        "permalink",
        "likes",
        "controversiality",
        "name",
        "created",
        "created_utc",
    ]
    df = df[selected_columns]
    return df


def df_to_sqlite(df, table):
    """Write a DataFrame to a SQLite database."""
    engine = create_engine("sqlite:///reddit.db", echo=False)
    print(f"Writing to {table} table in SQLite database...")
    df.to_sql(table, con=engine, if_exists="replace")


@click.command()
def reddit_data_to_sqlite():
    docs = pathlib.Path().parent / "data/docs"
    submissions_zst = docs / "submissions.zst"
    comments_zst = docs / "comments.zst"

    submissions_df = zst_to_df(submissions_zst)
    submissions_df = clean_up_submissions_df(submissions_df)
    df_to_sqlite(submissions_df, "submissions")

    comments_df = zst_to_df(comments_zst)
    comments_df = clean_up_comments(comments_df)
    df_to_sqlite(comments_df, "comments")


if __name__ == "__main__":
    reddit_data_to_sqlite()