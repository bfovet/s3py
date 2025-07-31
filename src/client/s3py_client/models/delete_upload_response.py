from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.upload_response import UploadResponse


T = TypeVar("T", bound="DeleteUploadResponse")


@_attrs_define
class DeleteUploadResponse:
    """
    Attributes:
        message (str):
        deleted_upload (UploadResponse):
    """

    message: str
    deleted_upload: "UploadResponse"
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        message = self.message

        deleted_upload = self.deleted_upload.to_dict()

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "message": message,
                "deleted_upload": deleted_upload,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.upload_response import UploadResponse

        d = dict(src_dict)
        message = d.pop("message")

        deleted_upload = UploadResponse.from_dict(d.pop("deleted_upload"))

        delete_upload_response = cls(
            message=message,
            deleted_upload=deleted_upload,
        )

        delete_upload_response.additional_properties = d
        return delete_upload_response

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
