import json

from pymilvus import (
    Collection,
    CollectionSchema,
    DataType,
    FieldSchema,
    connections,
    utility,
)
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

CHUNK = 10000


def load_authors() -> dict:
    authors = {}
    with open("author_key.txt", "r") as _in:
        for line in _in:
            pp = line[1:-1].split(",")
            authors[pp[0]] = ",".join(pp[1:]).strip()
    return authors


def load_data():
    print("Loading data")
    connections.connect("default", host="127.0.0.1", port="19530")

    authors = load_authors()

    author_id = {k: i for i, k in enumerate(authors.keys())}
    titles, authors, descriptions, title_index = [], [], [], []
    keyfail = 0
    with open("works_with_desc.txt", "r") as _in:
        for i, line in tqdm(enumerate(_in.readlines())):
            entry = json.loads(line.strip())
            if not ("title" in entry and "authors" in entry and "description" in entry):
                keyfail += 1
                continue
            try:
                authors.append(author_id[entry["authors"][0]["author"]["key"]])
            except KeyError:
                keyfail += 1
                continue
            title_index.append(i)
            titles.append(entry["title"])
            desc = (
                entry["description"]["value"]
                if isinstance(entry["description"], dict)
                else str(entry["description"])
            )
            descriptions.append(desc)
    print(f"{100*keyfail/(i+1):0.3f} percent skipped for missing keys")

    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
    # title_lk = dict(zip(title_index, titles))

    fields = [
        FieldSchema(
            name="title_id", dtype=DataType.INT64, is_primary=True, auto_id=False
        ),
        FieldSchema(name="author", dtype=DataType.INT64),
        FieldSchema(
            name="description",
            dtype=DataType.FLOAT_VECTOR,
            dim=len(model.encode([descriptions[0]])[0]),
        ),
    ]
    schema = CollectionSchema(fields, "All the books")
    utility.drop_collection("open_library")
    open_library = Collection("open_library", schema)
    print("Inserting data")
    for i in tqdm(range(0, len(authors), CHUNK)):
        insert_result = open_library.insert(
            [
                title_index[i : i + CHUNK],
                authors[i : i + CHUNK],
                model.encode(descriptions[i : i + CHUNK]),
            ]
        )
    print(insert_result)
    index = {
        "index_type": "IVF_FLAT",
        "metric_type": "L2",
        "params": {"nlist": 128},
    }
    open_library.create_index("description", index)


if __name__ == "__main__":
    load_data()
