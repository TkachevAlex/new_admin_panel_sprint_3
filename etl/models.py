from uuid import UUID
from typing import List, Optional
from pydantic import BaseModel


class Person(BaseModel):
    id: UUID
    name: str


class Movie(BaseModel):
    id: UUID
    imdb_rating: Optional[float]
    genres: List[str]
    title: str
    description: Optional[str]
    directors_names: List[str]
    actors_names: List[str]
    writers_names: List[str]
    directors: List[Person]
    actors: List[Person]
    writers: List[Person]
