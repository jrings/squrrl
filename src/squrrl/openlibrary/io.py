import json
import os
from typing import Optional

from ftlangdetect import detect
from tqdm import tqdm

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
        for i, line in tqdm(enumerate(_in), total=num_lines, position=0, leave=True):
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
    except (KeyError, IndexError):
        try:
            ref = entry["authors"][0]["key"]
        except (KeyError, IndexError):
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
        # log.debug(f"Could not find author with reference {ref}")
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
            for i, line in tqdm(enumerate(_in), total=num_lines, position=0, leave=True)
        )
    books = [b for b in books if b is not None]
    log.info(f"Found {len(books)} books")
    return authors, books
