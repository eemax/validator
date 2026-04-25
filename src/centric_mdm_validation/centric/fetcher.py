from pathlib import Path
from typing import Any

import httpx
import yaml
from pydantic import BaseModel, Field

from centric_mdm_validation.io import write_jsonl


class CentricEndpointConfig(BaseModel):
    name: str
    base_url: str
    path: str
    api_version: str = "v2"
    limit: int = 100
    query_params: dict[str, Any] = Field(default_factory=dict)
    token: str | None = None
    timeout: float = 30.0

    @property
    def url(self) -> str:
        return f"{self.base_url.rstrip('/')}/{self.api_version.strip('/')}/{self.path.lstrip('/')}"

    @classmethod
    def from_yaml(cls, path: Path, endpoint_name: str | None = None) -> "CentricEndpointConfig":
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        endpoints = data.get("endpoints") or []
        if not endpoints:
            raise ValueError(f"{path} does not define any endpoints.")

        endpoint = (
            next((item for item in endpoints if item.get("name") == endpoint_name), None)
            if endpoint_name
            else endpoints[0]
        )
        if endpoint is None:
            raise ValueError(f"Endpoint {endpoint_name!r} was not found in {path}.")

        merged = {
            "base_url": data["base_url"],
            "token": data.get("token"),
            "timeout": data.get("timeout", 30.0),
            **endpoint,
        }
        return cls.model_validate(merged)


def fetch_endpoint(config: CentricEndpointConfig, output_path: Path) -> int:
    headers = {"Authorization": f"Bearer {config.token}"} if config.token else {}
    records: list[dict[str, Any]] = []
    skip = 0

    with httpx.Client(timeout=config.timeout, headers=headers) as client:
        while True:
            response = client.get(
                config.url,
                params={**config.query_params, "skip": skip, "limit": config.limit},
            )
            response.raise_for_status()
            page = response.json()
            if not isinstance(page, list):
                raise ValueError(
                    f"Expected Centric endpoint {config.name!r} to return a list page."
                )
            if not page:
                break
            records.extend(page)
            if len(page) < config.limit:
                break
            skip += config.limit

    write_jsonl(output_path, records)
    return len(records)
