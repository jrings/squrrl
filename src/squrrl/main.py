import toml
import typer

from squrrl.openlibrary.io import load_data
from squrrl.vector.load_milvus import insert_books


def main(conf_fname: str):
    config = toml.load(conf_fname)
    authors, books = load_data(config)
    insert_books(config, authors, books)


if __name__ == "__main__":
    typer.run(main)
