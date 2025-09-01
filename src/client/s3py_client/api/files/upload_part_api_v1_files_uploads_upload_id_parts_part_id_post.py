from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.upload_part_response import UploadPartResponse
from ...types import UNSET, Response


def _get_kwargs(
    upload_id: str,
    part_id: int,
    *,
    etag: str,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["etag"] = etag

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": f"/api/v1/files/uploads/{upload_id}/parts/{part_id}",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, UploadPartResponse]]:
    if response.status_code == 201:
        response_201 = UploadPartResponse.from_dict(response.json())

        return response_201
    if response.status_code == 422:
        response_422 = HTTPValidationError.from_dict(response.json())

        return response_422
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[HTTPValidationError, UploadPartResponse]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    upload_id: str,
    part_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    etag: str,
) -> Response[Union[HTTPValidationError, UploadPartResponse]]:
    """Record a successfully uploaded part

     Updates the database with information about an uploaded part

    Args:
        upload_id (str):
        part_id (int):
        etag (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, UploadPartResponse]]
    """

    kwargs = _get_kwargs(
        upload_id=upload_id,
        part_id=part_id,
        etag=etag,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    upload_id: str,
    part_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    etag: str,
) -> Optional[Union[HTTPValidationError, UploadPartResponse]]:
    """Record a successfully uploaded part

     Updates the database with information about an uploaded part

    Args:
        upload_id (str):
        part_id (int):
        etag (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, UploadPartResponse]
    """

    return sync_detailed(
        upload_id=upload_id,
        part_id=part_id,
        client=client,
        etag=etag,
    ).parsed


async def asyncio_detailed(
    upload_id: str,
    part_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    etag: str,
) -> Response[Union[HTTPValidationError, UploadPartResponse]]:
    """Record a successfully uploaded part

     Updates the database with information about an uploaded part

    Args:
        upload_id (str):
        part_id (int):
        etag (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, UploadPartResponse]]
    """

    kwargs = _get_kwargs(
        upload_id=upload_id,
        part_id=part_id,
        etag=etag,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    upload_id: str,
    part_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    etag: str,
) -> Optional[Union[HTTPValidationError, UploadPartResponse]]:
    """Record a successfully uploaded part

     Updates the database with information about an uploaded part

    Args:
        upload_id (str):
        part_id (int):
        etag (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, UploadPartResponse]
    """

    return (
        await asyncio_detailed(
            upload_id=upload_id,
            part_id=part_id,
            client=client,
            etag=etag,
        )
    ).parsed
