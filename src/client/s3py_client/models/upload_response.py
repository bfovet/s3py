from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.upload_status import UploadStatus

T = TypeVar("T", bound="UploadResponse")


@_attrs_define
class UploadResponse:
    """
    Attributes:
        upload_id (str):
        user_id (str):
        key (str):
        status (UploadStatus):
    """

    upload_id: str
    user_id: str
    key: str
    status: UploadStatus
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        upload_id = self.upload_id

        user_id = self.user_id

        key = self.key

        status = self.status.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "upload_id": upload_id,
                "user_id": user_id,
                "key": key,
                "status": status,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        upload_id = d.pop("upload_id")

        user_id = d.pop("user_id")

        key = d.pop("key")

        status = UploadStatus(d.pop("status"))

        upload_response = cls(
            upload_id=upload_id,
            user_id=user_id,
            key=key,
            status=status,
        )

        upload_response.additional_properties = d
        return upload_response

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
