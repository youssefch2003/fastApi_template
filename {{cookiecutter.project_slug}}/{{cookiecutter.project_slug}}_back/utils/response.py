from collections.abc import Mapping
from typing import Any, Generic

from pydantic import Field, conlist

__all__ = (
    "ResponseMulti",
    "ResponseMultiPaginated",
    "Response",
    "_Response",
    "ErrorResponse",
    "ErrorResponseMulti"
)

from .entities import PublicEntity, _PublicEntity


class ResponseMulti(PublicEntity, Generic[_PublicEntity]):
    """Generic response model that consist multiple results."""

    result: list[_PublicEntity]


class ResponseMultiPaginated(PublicEntity, Generic[_PublicEntity]):
    """Generic response model that consist multiple results."""
    total_count: int
    page: int
    page_size: int
    result: list[_PublicEntity]


class Response(PublicEntity, Generic[_PublicEntity]):
    """Generic response model that consist only one result."""

    result: _PublicEntity


_Response = Mapping[int | str, dict[str, Any]]


class ErrorResponse(PublicEntity):
    """Error response model."""

    message: str = Field(description="This field represent the message")
    path: list = Field(
        description="The path to the field that raised the error",
        default_factory=list,
    )


class ErrorResponseMulti(PublicEntity):
    """The public error respnse model that includes multiple objects."""

    results: conlist(ErrorResponse, min_length=1)  # type: ignore
