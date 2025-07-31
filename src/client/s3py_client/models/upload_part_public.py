from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="UploadPartPublic")


@_attrs_define
class UploadPartPublic:
    """
    Attributes:
        upload_id (str):
        key (str):
        part_number (int):
        etag (str):
        user_id (str):
    """

    upload_id: str
    key: str
    part_number: int
    etag: str
    user_id: str
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        upload_id = self.upload_id

        key = self.key

        part_number = self.part_number

        etag = self.etag

        user_id = self.user_id

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "upload_id": upload_id,
                "key": key,
                "part_number": part_number,
                "etag": etag,
                "user_id": user_id,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        upload_id = d.pop("upload_id")

        key = d.pop("key")

        part_number = d.pop("part_number")

        etag = d.pop("etag")

        user_id = d.pop("user_id")

        upload_part_public = cls(
            upload_id=upload_id,
            key=key,
            part_number=part_number,
            etag=etag,
            user_id=user_id,
        )

        upload_part_public.additional_properties = d
        return upload_part_public

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
