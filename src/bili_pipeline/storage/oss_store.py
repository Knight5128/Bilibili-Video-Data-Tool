from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

import oss2

from bili_pipeline.models import OSSStorageConfig


@dataclass(slots=True)
class OSSUploadCommitResult:
    etag: str
    object_url: str
    part_count: int


@dataclass(slots=True)
class OSSMultipartUploadSession:
    bucket: oss2.Bucket
    config: OSSStorageConfig
    object_key: str
    upload_id: str
    headers: dict[str, str] = field(default_factory=dict)
    _parts: list[oss2.models.PartInfo] = field(default_factory=list)

    def upload_part(self, chunk: bytes) -> None:
        part_number = len(self._parts) + 1
        result = self.bucket.upload_part(self.object_key, self.upload_id, part_number, chunk)
        self._parts.append(oss2.models.PartInfo(part_number, result.etag))

    def complete(self) -> OSSUploadCommitResult:
        result = self.bucket.complete_multipart_upload(
            self.object_key,
            self.upload_id,
            self._parts,
            headers=self.headers,
        )
        return OSSUploadCommitResult(
            etag=getattr(result, "etag", ""),
            object_url=_build_object_url(self.config, self.object_key),
            part_count=len(self._parts),
        )

    def abort(self) -> None:
        self.bucket.abort_multipart_upload(self.object_key, self.upload_id)

    @property
    def part_count(self) -> int:
        return len(self._parts)


def _build_auth(config: OSSStorageConfig) -> oss2.Auth:
    if config.security_token.strip():
        return oss2.StsAuth(
            config.access_key_id.strip(),
            config.access_key_secret.strip(),
            config.security_token.strip(),
        )
    return oss2.Auth(config.access_key_id.strip(), config.access_key_secret.strip())


def _build_object_url(config: OSSStorageConfig, object_key: str) -> str:
    key = quote(object_key.lstrip("/"))
    public_base_url = config.public_base_url.strip().rstrip("/")
    if public_base_url:
        return f"{public_base_url}/{key}"

    endpoint = config.endpoint_with_scheme()
    if not endpoint:
        return ""
    scheme, _, host = endpoint.partition("://")
    return f"{scheme}://{config.bucket_name.strip()}.{host}/{key}"


class OSSMediaStore:
    def __init__(self, config: OSSStorageConfig) -> None:
        if not config.is_enabled():
            raise ValueError("OSS configuration is incomplete.")
        self.config = config
        self.bucket = oss2.Bucket(
            _build_auth(config),
            config.endpoint_with_scheme(),
            config.bucket_name.strip(),
        )

    @property
    def bucket_name(self) -> str:
        return self.config.bucket_name.strip()

    @property
    def endpoint(self) -> str:
        return self.config.endpoint_with_scheme()

    def build_object_url(self, object_key: str) -> str:
        return _build_object_url(self.config, object_key)

    def create_multipart_upload(
        self,
        *,
        object_key: str,
        mime_type: str,
        metadata: dict[str, str] | None = None,
    ) -> OSSMultipartUploadSession:
        headers = {"Content-Type": mime_type}
        if metadata:
            for key, value in metadata.items():
                headers[f"x-oss-meta-{key}"] = value
        result = self.bucket.init_multipart_upload(object_key, headers=headers)
        return OSSMultipartUploadSession(
            bucket=self.bucket,
            config=self.config,
            object_key=object_key,
            upload_id=result.upload_id,
            headers=headers,
        )

    def download_object_bytes(self, object_key: str) -> bytes:
        return self.bucket.get_object(object_key).read()

    def build_signed_url(self, object_key: str, expires_seconds: int = 3600) -> str:
        return self.bucket.sign_url("GET", object_key, expires_seconds)

    def test_connection(self) -> dict[str, Any]:
        result = self.bucket.get_bucket_info()
        bucket_info = getattr(result, "bucket_info", None)
        return {
            "bucket_name": getattr(bucket_info, "name", self.bucket_name),
            "region": getattr(bucket_info, "location", self.config.bucket_region.strip()),
            "storage_class": getattr(bucket_info, "storage_class", None),
            "public_base_url": self.config.public_base_url.strip() or None,
        }
