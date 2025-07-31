from http import HTTPStatus
from typing import Any, Optional, Union

import httpx

from ... import errors
from ...client import AuthenticatedClient, Client
from ...models.http_validation_error import HTTPValidationError
from ...models.upload_response import UploadResponse
from ...models.upload_status import UploadStatus
from ...types import UNSET, Response, Unset


def _get_kwargs(
    *,
    key: str,
    upload_status: Union[None, Unset, list[UploadStatus]] = UNSET,
) -> dict[str, Any]:
    params: dict[str, Any] = {}

    params["key"] = key

    json_upload_status: Union[None, Unset, list[str]]
    if isinstance(upload_status, Unset):
        json_upload_status = UNSET
    elif isinstance(upload_status, list):
        json_upload_status = []
        for upload_status_type_0_item_data in upload_status:
            upload_status_type_0_item = upload_status_type_0_item_data.value
            json_upload_status.append(upload_status_type_0_item)

    else:
        json_upload_status = upload_status
    params["upload_status"] = json_upload_status

    params = {k: v for k, v in params.items() if v is not UNSET and v is not None}

    _kwargs: dict[str, Any] = {
        "method": "get",
        "url": "/api/v1/uploads",
        "params": params,
    }

    return _kwargs


def _parse_response(
    *, client: Union[AuthenticatedClient, Client], response: httpx.Response
) -> Optional[Union[HTTPValidationError, list["UploadResponse"]]]:
    if response.status_code == 200:
        response_200 = []
        _response_200 = response.json()
        for response_200_item_data in _response_200:
            response_200_item = UploadResponse.from_dict(response_200_item_data)

            response_200.append(response_200_item)

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
) -> Response[Union[HTTPValidationError, list["UploadResponse"]]]:
    return Response(
        status_code=HTTPStatus(response.status_code),
        content=response.content,
        headers=response.headers,
        parsed=_parse_response(client=client, response=response),
    )


def sync_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    key: str,
    upload_status: Union[None, Unset, list[UploadStatus]] = UNSET,
) -> Response[Union[HTTPValidationError, list["UploadResponse"]]]:
    """List all uploads for a file

     Gets a list of uploads for a file, optionally filtered by status

    Args:
        key (str):
        upload_status (Union[None, Unset, list[UploadStatus]]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['UploadResponse']]]
    """

    kwargs = _get_kwargs(
        key=key,
        upload_status=upload_status,
    )

    response = client.get_httpx_client().request(
        **kwargs,
    )

    return _build_response(client=client, response=response)


def sync(
    *,
    client: Union[AuthenticatedClient, Client],
    key: str,
    upload_status: Union[None, Unset, list[UploadStatus]] = UNSET,
) -> Optional[Union[HTTPValidationError, list["UploadResponse"]]]:
    """List all uploads for a file

     Gets a list of uploads for a file, optionally filtered by status

    Args:
        key (str):
        upload_status (Union[None, Unset, list[UploadStatus]]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['UploadResponse']]
    """

    return sync_detailed(
        client=client,
        key=key,
        upload_status=upload_status,
    ).parsed


async def asyncio_detailed(
    *,
    client: Union[AuthenticatedClient, Client],
    key: str,
    upload_status: Union[None, Unset, list[UploadStatus]] = UNSET,
) -> Response[Union[HTTPValidationError, list["UploadResponse"]]]:
    """List all uploads for a file

     Gets a list of uploads for a file, optionally filtered by status

    Args:
        key (str):
        upload_status (Union[None, Unset, list[UploadStatus]]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Response[Union[HTTPValidationError, list['UploadResponse']]]
    """

    kwargs = _get_kwargs(
        key=key,
        upload_status=upload_status,
    )

    response = await client.get_async_httpx_client().request(**kwargs)

    return _build_response(client=client, response=response)


async def asyncio(
    *,
    client: Union[AuthenticatedClient, Client],
    key: str,
    upload_status: Union[None, Unset, list[UploadStatus]] = UNSET,
) -> Optional[Union[HTTPValidationError, list["UploadResponse"]]]:
    """List all uploads for a file

     Gets a list of uploads for a file, optionally filtered by status

    Args:
        key (str):
        upload_status (Union[None, Unset, list[UploadStatus]]):

    Raises:
        errors.UnexpectedStatus: If the server returns an undocumented status code and Client.raise_on_unexpected_status is True.
        httpx.TimeoutException: If the request takes longer than Client.timeout.

    Returns:
        Union[HTTPValidationError, list['UploadResponse']]
    """

    return (
        await asyncio_detailed(
            client=client,
            key=key,
            upload_status=upload_status,
        )
    ).parsed
