from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)
from sentence_transformers import SentenceTransformer
from tqdm import trange

from squrrl.dataclasses import AuthorCollection, Book
from squrrl.tools import log


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
        config.get("base", {}).get("insert_startpoint", 0),
        len(books) + 1,
        chunk,
        position=0,
        leave=True,
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
    log.info("Creating Index")
    index = {
        "index_type": "FLAT",
        "metric_type": "COSINE",
        "params": {"nlist": 32768},
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
