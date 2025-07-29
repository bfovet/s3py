from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.get_presigned_url_api_v1_presigned_url_get_response_get_presigned_url_api_v1_presigned_url_get import (
    GetPresignedUrlApiV1PresignedUrlGetResponseGetPresignedUrlApiV1PresignedUrlGet,
)
from ...models.http_validation_error import HTTPValidationError
from ...types import UNSET, Response


def _get_kwargs(
    *,
    upload_id: str,
    key: str,
    part_number: int,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["upload_id"] = upload_id

    params["key"] = key

    params["part_number"] = part_number

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/presigned-url",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[
    Union[
        GetPresignedUrlApiV1PresignedUrlGetResponseGetPresignedUrlApiV1PresignedUrlGet,
        HTTPValidationError,
    ]
]:
    if response.status_code == 200:
        response_200 = GetPresignedUrlApiV1PresignedUrlGetResponseGetPresignedUrlApiV1PresignedUrlGet.from_dict(
            response.json()
        )

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
) -> Response[
    Union[
        GetPresignedUrlApiV1PresignedUrlGetResponseGetPresignedUrlApiV1PresignedUrlGet,
        HTTPValidationError,
    ]
]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    upload_id: str,
    key: str,
    part_number: int,
) -> Response[
    Union[
        GetPresignedUrlApiV1PresignedUrlGetResponseGetPresignedUrlApiV1PresignedUrlGet,
        HTTPValidationError,
    ]
]:
    """Get a presigned URL for part upload

     Generates a presigned URL that allows direct upload to S3

    Args:
        upload_id (str):
        key (str):
        part_number (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[GetPresignedUrlApiV1PresignedUrlGetResponseGetPresignedUrlApiV1PresignedUrlGet, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        upload_id=upload_id,
        key=key,
        part_number=part_number,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    upload_id: str,
    key: str,
    part_number: int,
) -> Optional[
    Union[
        GetPresignedUrlApiV1PresignedUrlGetResponseGetPresignedUrlApiV1PresignedUrlGet,
        HTTPValidationError,
    ]
]:
    """Get a presigned URL for part upload

     Generates a presigned URL that allows direct upload to S3

    Args:
        upload_id (str):
        key (str):
        part_number (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[GetPresignedUrlApiV1PresignedUrlGetResponseGetPresignedUrlApiV1PresignedUrlGet, HTTPValidationError]
    """

    return sync_detailed(
        client=client,
        upload_id=upload_id,
        key=key,
        part_number=part_number,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    upload_id: str,
    key: str,
    part_number: int,
) -> Response[
    Union[
        GetPresignedUrlApiV1PresignedUrlGetResponseGetPresignedUrlApiV1PresignedUrlGet,
        HTTPValidationError,
    ]
]:
    """Get a presigned URL for part upload

     Generates a presigned URL that allows direct upload to S3

    Args:
        upload_id (str):
        key (str):
        part_number (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[GetPresignedUrlApiV1PresignedUrlGetResponseGetPresignedUrlApiV1PresignedUrlGet, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        upload_id=upload_id,
        key=key,
        part_number=part_number,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    upload_id: str,
    key: str,
    part_number: int,
) -> Optional[
    Union[
        GetPresignedUrlApiV1PresignedUrlGetResponseGetPresignedUrlApiV1PresignedUrlGet,
        HTTPValidationError,
    ]
]:
    """Get a presigned URL for part upload

     Generates a presigned URL that allows direct upload to S3

    Args:
        upload_id (str):
        key (str):
        part_number (int):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[GetPresignedUrlApiV1PresignedUrlGetResponseGetPresignedUrlApiV1PresignedUrlGet, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            client=client,
            upload_id=upload_id,
            key=key,
            part_number=part_number,
        )
    ).parsed
