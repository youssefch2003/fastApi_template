from sqlalchemy.exc import IntegrityError, InvalidRequestError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from {{cookiecutter.project_slug}}.db.base import ConcreteTable
from .errors import DatabaseError, NotFoundError, UnprocessableError
from typing import Any, AsyncGenerator, Generic, Type, Optional, Dict, List
from sqlalchemy import asc, delete, desc, func, select, update, or_
from sqlalchemy.engine import Result
from asyncpg.exceptions import UniqueViolationError

__all__ = ("BaseRepository",)

from .tools import build_filters


class BaseRepository(Generic[ConcreteTable]):
    """This class implements the base interface for working with the database
    and makes it easier to work with type annotations.

    The Session class implements the database interaction layer.
    """

    schema_class: Type[ConcreteTable]
    _ERRORS = (IntegrityError, InvalidRequestError, UniqueViolationError)

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        super().__init__()
        if not self.schema_class:
            raise UnprocessableError(
                message=(
                    "Cannot initiate the class without schema_class attribute"
                )
            )

    async def _update(
        self, key: str, value: Any, payload: dict[str, Any]
    ) -> ConcreteTable:
        """Updates an existing instance of the model in the related table.
        If some data does not exist in the payload, then the null value will
        be passed to the schema class."""

        query = (
            update(self.schema_class)
            .where(getattr(self.schema_class, key) == value)
            .values(payload)
            .returning(self.schema_class)
        )
        result: Result = await self._session.execute(query)

        if not (schema := result.scalar_one_or_none()):
            raise DatabaseError

        return schema

    async def _get(self, id_: int, relationships: Optional[List[str]] = None) -> ConcreteTable:
        query = select(self.schema_class).where(self.schema_class.id == id_)

        if relationships:
            for relationship in relationships:
                query = query.options(selectinload(getattr(self.schema_class, relationship)))

        result: Result = await self._session.execute(query)

        if not (instance := result.scalars().one_or_none()):
            raise NotFoundError

        return instance

    async def count(self) -> int:
        result: Result = await self._session.execute(func.count(self.schema_class.id))
        value = result.scalar()

        if not isinstance(value, int):
            raise UnprocessableError(
                message=(
                    "For some reason count function returned not an integer."
                    f"Value: {value}"
                ),
            )

        return value

    async def find_by(self, filters: Dict[str, Any], relationships: Optional[List[str]] = None) -> Optional[
         ConcreteTable]:
        """Find a single instance of the model by dynamic attributes.

        Args:
            filters (Dict[str, Any]): A dictionary of attributes and their corresponding values to filter by.
            relationships (Optional[List[str]]): A list of relationship names to eagerly load.

        Returns:
            Optional[ConcreteTable]: The found instance or None if no instance is found.
        """
        query = select(self.schema_class)

        # Apply relationships if provided
        if relationships:
            for relationship in relationships:
                query = query.options(selectinload(getattr(self.schema_class, relationship)))

        # Apply filters dynamically
        conditions = [getattr(self.schema_class, key) == value for key, value in filters.items()]
        query = query.where(*conditions)

        result: Result = await self._session.execute(query)
        return result.scalar_one_or_none()

    async def _first(self, by: str = "id") -> ConcreteTable:
        result: Result = await self._session.execute(
            select(self.schema_class).order_by(asc(by)).limit(1)
        )

        if not (_result := result.scalar_one_or_none()):
            raise NotFoundError

        return _result

    async def _last(self, by: str = "id") -> ConcreteTable:
        result: Result = await self._session.execute(
            select(self.schema_class).order_by(desc(by)).limit(1)
        )

        if not (_result := result.scalar_one_or_none()):
            raise NotFoundError

        return _result

    async def _save(self, payload: dict[str, Any]) -> ConcreteTable:
        try:
            schema = self.schema_class(**payload)
            self._session.add(schema)
            await self._session.flush()
            await self._session.refresh(schema)
            return schema
        except self._ERRORS as e:
            print(e)
            raise DatabaseError

    async def _all(self) -> AsyncGenerator[ConcreteTable, None]:
        result: Result = await self._session.execute(select(self.schema_class))
        schemas = result.scalars().all()

        for schema in schemas:
            yield schema

    async def _all_paginated(
        self,
        page: int,
        page_size: int,
        relationships: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        if relationships is None:
            relationships = []

        query = select(self.schema_class)

        for relationship in relationships:
            query = query.options(selectinload(getattr(self.schema_class, relationship)))

        if filters:
            query = build_filters(self.schema_class, query, filters)

        total_count_query = query.with_only_columns(func.count()).order_by(None)
        total_count_result: Result = await self._session.execute(total_count_query)
        total_count = total_count_result.scalar()

        offset = (page - 1) * page_size
        query = query.limit(page_size).offset(offset)

        result: Result = await self._session.execute(query)
        schemas = result.scalars().all()

        return {
            "total_count": total_count,
            "page": page,
            "page_size": page_size,
            "result": schemas
        }

    async def _delete(self, id_: int) -> None:
        await self._session.execute(
            delete(self.schema_class).where(self.schema_class.id == id_)
        )

    async def _exists(self, filters: dict[str, Any]) -> bool:
        """Check if a record exists by given filters using OR operator."""

        try:
            conditions = [
                getattr(self.schema_class, key) == (value.strip().lower() if isinstance(value, str) else value)
                for key, value in filters.items()
            ]
        except AttributeError as e:
            raise UnprocessableError(
                message=f"Invalid attribute in filters: {str(e)}"
            )
        query = select(self.schema_class).where(or_(*conditions))
        result: Result = await self._session.execute(query)
        return result.scalars().first() is not None
