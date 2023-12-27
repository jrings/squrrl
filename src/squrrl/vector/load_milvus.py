import json
import os
from typing import Optional

import toml
import typer
from ftlangdetect import detect
from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)
from sentence_transformers import SentenceTransformer
from tqdm import tqdm, trange

from squrrl.dataclasses import Author, AuthorCollection, Book
from squrrl.tools import log


def load_authors(config: dict) -> dict:
    """Load the author names, ids and reference string"""
    log.debug("Reading authors file")
    authors = []
    author_fname = os.path.join(config["base"]["directory"], "author_key.txt")
    with open(author_fname, "rb") as f:
        num_lines = sum(1 for _ in f)
    with open(author_fname, "r") as _in:
        for i, line in tqdm(enumerate(_in), total=num_lines):
            pp = line[1:-1].split(",")
            a = Author(name=",".join(pp[1:]).strip(), aid=i, reference=pp[0])
            authors.append(a)
    return AuthorCollection(authors)


def read_entry(entry: str, i: int, authors: AuthorCollection) -> Optional[Book]:
    """
    Reads an entry and returns a Book object.

    Args:
        entry: The entry to be read.
        i: Id to assign

    Returns:
        A Book object representing the entry.
    """
    entry = json.loads(entry)
    if "title" not in entry or "authors" not in entry or "description" not in entry:
        return None
    try:
        ref = entry["authors"][0]["author"]["key"]
    except KeyError:
        return None

    desc = (
        entry["description"]["value"]
        if isinstance(entry["description"], dict)
        else str(entry["description"])
    )
    desc = desc.replace("\n", " ")
    lang = detect(desc)["lang"]
    if lang != "en":
        return None
    try:
        author = authors.find(ref)
    except KeyError:
        log.debug(f"Could not find author with reference {ref}")
        return None
    return Book(title=entry["title"], author=author, bid=i, description=desc)


def load_data(config: dict) -> tuple[AuthorCollection, list[Book]]:
    """Load the data from the OpenLibrary dump into milvus."""
    log.debug("Loading data")
    authors = load_authors(config)
    log.info("Reading works file")
    books = []
    works_fname = os.path.join(config["base"]["directory"], "works_with_desc.txt")
    with open(works_fname, "rb") as f:
        num_lines = sum(1 for _ in f)
    with open(works_fname, "r") as _in:
        books.extend(
            read_entry(line.strip(), i, authors)
            for i, line in tqdm(enumerate(_in), total=num_lines)
        )
    books = [b for b in books if b is not None]
    log.info(f"Found {len(books)} books")
    return authors, books


def insert_books(config: dict, authors: AuthorCollection, books: list[Book]) -> None:
    model = SentenceTransformer(
        config.get("base", {}).get("transformer_model", "paraphrase-MiniLM-L3-v2"),
        device="cuda",
    )

    fields = [
        FieldSchema(
            name="title_id", dtype=DataType.INT64, is_primary=True, auto_id=False
        ),
        FieldSchema(name="author", dtype=DataType.INT64),
        FieldSchema(
            name="description",
            dtype=DataType.FLOAT_VECTOR,
            dim=len(model.encode([books[0].description])[0]),
        ),
    ]

    log.info(f"Inserting {len(books)} books into Milvus")
    connections.connect("default", host="127.0.0.1", port="19530")
    schema = CollectionSchema(fields, "All the books")
    utility.drop_collection("open_library")
    open_library = Collection("open_library", schema)
    chunk = config.get("base", {}).get("chunk_size", 10000)
    for i in trange(
        config.get("base", {}).get("insert_startpoint", 0), len(books) + 1, chunk
    ):
        book_chunk = books[i : i + chunk]
        fields = [
            [b.bid for b in book_chunk],
            [b.author.aid for b in book_chunk],
            model.encode([b.description for b in book_chunk]),
        ]
        validate_fields(fields)
        insert_result = open_library.insert(fields)
        log.debug(insert_result)
    index = {
        "index_type": "IVF_FLAT",
        "metric_type": "L2",
        "params": {"nlist": 128},
    }
    open_library.create_index("description", index)
    log.info("Done!")


def validate_fields(fields: list[list]) -> None:
    """Validate the fields to be inserted into Milvus."""
    if not len(fields[0]):
        raise ValueError("Fields must not be empty")
    for field in fields:
        if len(field) != len(fields[0]):
            raise ValueError(
                f"Fields must have the same length, are {[len(f) for f in fields]}"
            )
        if any([f is None for f in field]):
            raise ValueError("Fields must not contain None values")
        if sum([len(set([type(x) for x in f])) for f in fields]) != 3:
            raise ValueError(
                f"Fields must have the same type, got {[set([type(f) for f in fi]) for fi in fields]}"
            )


def main(conf_fname: str):
    config = toml.load(conf_fname)
    authors, books = load_data(config)
    insert_books(config, authors, books)


if __name__ == "__main__":
    typer.run(main)
