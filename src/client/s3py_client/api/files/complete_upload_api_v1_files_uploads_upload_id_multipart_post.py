from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.complete_upload_response import CompleteUploadResponse
from ...models.http_validation_error import HTTPValidationError
from ...types import Response


def _get_kwargs(
    upload_id: str,
) -> dict[str, Any]:
    _kwargs: dict[str, Any] = {
        "method": "post",
        "url": f"/api/v1/files/uploads/{upload_id}/multipart",
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[CompleteUploadResponse, HTTPValidationError]]:
    if response.status_code == 201:
        response_201 = CompleteUploadResponse.from_dict(response.json())

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
) -> Response[Union[CompleteUploadResponse, HTTPValidationError]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    upload_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[CompleteUploadResponse, HTTPValidationError]]:
    """Complete the multipart upload

     Finalizes the multipart upload by combining all parts in S3

    Args:
        upload_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CompleteUploadResponse, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        upload_id=upload_id,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    upload_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[CompleteUploadResponse, HTTPValidationError]]:
    """Complete the multipart upload

     Finalizes the multipart upload by combining all parts in S3

    Args:
        upload_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CompleteUploadResponse, HTTPValidationError]
    """

    return sync_detailed(
        upload_id=upload_id,
        client=client,
    ).parsed


async def asyncio_detailed(
    upload_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Response[Union[CompleteUploadResponse, HTTPValidationError]]:
    """Complete the multipart upload

     Finalizes the multipart upload by combining all parts in S3

    Args:
        upload_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[CompleteUploadResponse, HTTPValidationError]]
    """

    kwargs = _get_kwargs(
        upload_id=upload_id,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    upload_id: str,
    *,
    client: Union[AuthenticatedClient, Client],
) -> Optional[Union[CompleteUploadResponse, HTTPValidationError]]:
    """Complete the multipart upload

     Finalizes the multipart upload by combining all parts in S3

    Args:
        upload_id (str):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[CompleteUploadResponse, HTTPValidationError]
    """

    return (
        await asyncio_detailed(
            upload_id=upload_id,
            client=client,
        )
    ).parsed
