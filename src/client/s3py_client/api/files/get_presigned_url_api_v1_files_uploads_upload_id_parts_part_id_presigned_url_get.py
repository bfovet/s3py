from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.presigned_url_response import PresignedUrlResponse
from ...types import UNSET, Response


def _get_kwargs(
    upload_id: str,
    part_id: int,
    *,
    key: str,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["key"] = key

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": f"/api/v1/files/uploads/{upload_id}/parts/{part_id}/presigned-url",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, PresignedUrlResponse]]:
    if response.status_code == 200:
        response_200 = PresignedUrlResponse.from_dict(response.json())

        return response_200
    if response.status_code == 422:
        response_422 = HTTPValidationError.from_dict(response.json())

        return response_422
    if client.raise_on_unexpected_status:
        raise errors.UnexpectedStatus(response.status_code, response.content)
    else:
        return None


def _build_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Response[Union[HTTPValidationError, PresignedUrlResponse]]:
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
    key: str,
) -> Response[Union[HTTPValidationError, PresignedUrlResponse]]:
    """Get a presigned URL for part upload

     Generates a presigned URL that allows direct upload to S3

    Args:
        upload_id (str):
        part_id (int):
        key (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, PresignedUrlResponse]]
    """

    kwargs = _get_kwargs(
        upload_id=upload_id,
        part_id=part_id,
        key=key,
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
    key: str,
) -> Optional[Union[HTTPValidationError, PresignedUrlResponse]]:
    """Get a presigned URL for part upload

     Generates a presigned URL that allows direct upload to S3

    Args:
        upload_id (str):
        part_id (int):
        key (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, PresignedUrlResponse]
    """

    return sync_detailed(
        upload_id=upload_id,
        part_id=part_id,
        client=client,
        key=key,
    ).parsed


async def asyncio_detailed(
    upload_id: str,
    part_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    key: str,
) -> Response[Union[HTTPValidationError, PresignedUrlResponse]]:
    """Get a presigned URL for part upload

     Generates a presigned URL that allows direct upload to S3

    Args:
        upload_id (str):
        part_id (int):
        key (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, PresignedUrlResponse]]
    """

    kwargs = _get_kwargs(
        upload_id=upload_id,
        part_id=part_id,
        key=key,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    upload_id: str,
    part_id: int,
    *,
    client: Union[AuthenticatedClient, Client],
    key: str,
) -> Optional[Union[HTTPValidationError, PresignedUrlResponse]]:
    """Get a presigned URL for part upload

     Generates a presigned URL that allows direct upload to S3

    Args:
        upload_id (str):
        part_id (int):
        key (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, PresignedUrlResponse]
    """

    return (
        await asyncio_detailed(
            upload_id=upload_id,
            part_id=part_id,
            client=client,
            key=key,
        )
    ).parsed
