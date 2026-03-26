from pydantic import BaseModel


class SearchRequest(BaseModel):
    query: str


class AlertRequest(BaseModel):
    keyword: str