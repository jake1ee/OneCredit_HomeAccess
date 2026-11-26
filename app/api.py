import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated

import psycopg
from fastapi import FastAPI, HTTPException, Path, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool, PoolTimeout
from pydantic import BaseModel, Field

from app import repository
from app.config import Settings, get_settings
from app.db import create_pool, ensure_schema
from app.events import InvalidEvent, normalise_registration_number
from app.logging_setup import setup_logging

log = logging.getLogger("app.api")

_CACHE_CONTROL = "public, max-age=60"


class CompanyCard(BaseModel):
    registration_number: str = Field(serialization_alias="registrationNumber")
    company_name: str = Field(serialization_alias="companyName")
    industry: str | None
    state: str | None
    similarity: float = Field(description="Cosine similarity of the business descriptions, 1 = identical.")


class Company(BaseModel):
    registration_number: str = Field(serialization_alias="registrationNumber")
    company_name: str | None = Field(serialization_alias="companyName")
    nature_of_business: str | None = Field(serialization_alias="natureOfBusiness")
    industry: str | None
    state: str | None
    is_struck_off: bool = Field(serialization_alias="isStruckOff")
    source_updated_at: datetime = Field(serialization_alias="updatedAt")


class SimilarCompanies(BaseModel):
    registration_number: str = Field(serialization_alias="registrationNumber")
    similar: list[CompanyCard] = Field(description="Most similar first. Struck-off companies are never included.")


class ErrorBody(BaseModel):
    code: str
    message: str


class ErrorResponse(BaseModel):
    error: ErrorBody


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        self.status, self.code, self.message = status, code, message


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        setup_logging(settings.log_level)
        try:
            await ensure_schema(settings.database_url)
        except psycopg.OperationalError as exc:
            log.error("could not prepare schema at startup: %s", exc)
        pool = create_pool(
            settings.database_url,
            min_size=settings.db_pool_min_size,
            max_size=settings.db_pool_max_size,
            statement_timeout_ms=settings.db_statement_timeout_ms,
            hnsw_ef_search=settings.hnsw_ef_search,
            timeout=settings.db_pool_timeout_seconds,  # fail fast with 503 rather than hang the page load
        )
        await pool.open(wait=False)
        app.state.pool = pool
        try:
            yield
        finally:
            await pool.close()

    app = FastAPI(
        title="Similar companies",
        version="1.0.0",
        lifespan=lifespan,
        responses={503: {"model": ErrorResponse, "description": "Database unavailable"}},
    )

    @app.exception_handler(ApiError)
    async def _api_error(_: Request, exc: ApiError) -> JSONResponse:
        return _error(exc.status, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def _validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
        first = exc.errors()[0]
        where = ".".join(str(p) for p in first.get("loc", ())[1:])
        return _error(422, "invalid_request", f"{where}: {first.get('msg')}")

    @app.exception_handler(HTTPException)
    async def _http_error(_: Request, exc: HTTPException) -> JSONResponse:
        return _error(exc.status_code, "http_error", str(exc.detail))

    @app.exception_handler(psycopg.OperationalError)
    @app.exception_handler(PoolTimeout)
    async def _db_unavailable(_: Request, exc: Exception) -> JSONResponse:
        log.error("database unavailable: %s", exc)
        return _error(503, "database_unavailable", "Recommendations are temporarily unavailable.")

    def pool_of(request: Request) -> AsyncConnectionPool:
        return request.app.state.pool

    RegistrationNumber = Annotated[str, Path(description="Registration number, e.g. SYN-001550")]

    @app.get(
        "/companies/{registration_number}/similar",
        response_model=SimilarCompanies,
        response_model_by_alias=True,
        responses={404: {"model": ErrorResponse}, 422: {"model": ErrorResponse}},
        summary="Most similar companies, for the portal's 'Similar companies' panel",
    )
    async def similar_companies(
        request: Request,
        response: Response,
        registration_number: RegistrationNumber,
        limit: Annotated[int, Query(ge=1, le=settings.similar_max_limit)] = settings.similar_default_limit,
    ) -> SimilarCompanies:
        reg = _parse_registration_number(registration_number)
        async with pool_of(request).connection() as conn:
            company = await repository.get_company(conn, reg)
            if company is None or company["company_name"] is None:  # unknown, or only a strike-off tombstone
                raise ApiError(404, "company_not_found", f"No company with registration number {reg}.")
            rows = await repository.find_similar(conn, reg, limit) if company["has_embedding"] else []
        response.headers["Cache-Control"] = _CACHE_CONTROL
        return SimilarCompanies(registration_number=reg, similar=[CompanyCard(**row) for row in rows])

    @app.get(
        "/companies/{registration_number}",
        response_model=Company,
        response_model_by_alias=True,
        responses={404: {"model": ErrorResponse}, 422: {"model": ErrorResponse}},
        summary="One company's details",
    )
    async def get_company(request: Request, response: Response, registration_number: RegistrationNumber) -> Company:
        reg = _parse_registration_number(registration_number)
        async with pool_of(request).connection() as conn:
            company = await repository.get_company(conn, reg)
        if company is None or company["company_name"] is None:
            raise ApiError(404, "company_not_found", f"No company with registration number {reg}.")
        response.headers["Cache-Control"] = _CACHE_CONTROL
        return Company(**company)

    @app.get("/health/live", include_in_schema=False)
    async def live() -> dict:
        return {"status": "ok"}

    @app.get("/health/ready", include_in_schema=False)
    async def ready(request: Request) -> JSONResponse:
        """Ready when Postgres answers. Reports how many companies can be recommended, for dashboards."""
        try:
            async with pool_of(request).connection(timeout=2) as conn:
                cur = await conn.execute("SELECT count(*) FILTER (WHERE embedding IS NOT NULL) FROM companies")
                (indexed,) = await cur.fetchone()
        except (psycopg.Error, PoolTimeout) as exc:
            return _error(503, "not_ready", str(exc))
        return JSONResponse({"status": "ok", "indexedCompanies": indexed})

    return app


def _parse_registration_number(value: str) -> str:
    try:
        return normalise_registration_number(value)
    except InvalidEvent:
        raise ApiError(
            422, "invalid_registration_number", f"'{value}' is not a registration number like SYN-001550."
        ) from None


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


app = create_app()
