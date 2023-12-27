from dataclasses import dataclass
from typing import Union


@dataclass
class Author:
    name: str
    aid: int
    reference: str


@dataclass
class Book:
    title: str
    author: Author
    bid: int
    description: str
    language: str = "en"


class AuthorCollection:
    _authors: list[Author] = []
    author_lk_by_ref: dict[str, Author] = {}
    author_lk_by_id: dict[int, Author] = {}

    def __init__(self, authors: Union[Author, list[Author]]):
        self.add_authors(authors)
        self._update_lookups()

    def __len__(self):
        return len(self._authors)

    def _check_valid(self, authors: Union[Author, list[Author]]) -> None:
        """
        Checks if the given authors are valid.

        Args:
            authors: An Author instance or a list of Author instances.

        Raises:
            TypeError: If authors is not an Author instance or a list thereof.
        """
        if isinstance(authors, list):
            [self._check_valid(a) for a in authors]
        elif not isinstance(authors, Author):
            raise TypeError("authors must be an Author instance or list thereof")
        return True

    def _update_lookups(self):
        """Updates the author lookup dictionaries."""
        self.author_lk_by_ref = {author.reference: author for author in self._authors}
        self.author_lk_by_id = {author.aid: author for author in self._authors}

    def add_authors(self, authors: Union[Author, list[Author]]) -> None:
        self._check_valid(authors)
        if isinstance(authors, Author):
            self._authors.append(authors)
        else:
            self._authors.extend(authors)
        self._update_lookups()

    def find(self, ref: Union[str, int]) -> Author:
        """Find author instance by reference or aid."""
        if isinstance(ref, str):
            return self.author_lk_by_ref[ref]
        else:
            return self.author_lk_by_id[ref]
