from centric_mdm_validation.centric.reconstruction import (
    reconstruct_products_from_records,
    resolve_reconstruction_path,
)


def test_reconstruct_products_from_records_uses_private_module(tmp_path) -> None:
    module_path = tmp_path / "reconstruction.py"
    module_path.write_text(
        """
from centric_mdm_validation.models import CentricProductPayload


def reconstruct_projected_products(records_by_endpoint, *, mapping=None):
    return [CentricProductPayload(centric_style_id="PRIVATE-1")]
""",
        encoding="utf-8",
    )

    payloads = reconstruct_products_from_records({}, reconstruction_path=module_path)

    assert payloads[0].centric_style_id == "PRIVATE-1"


def test_reconstruct_products_from_records_falls_back_to_public_mapper(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.chdir(tmp_path)

    payloads = reconstruct_products_from_records(
        {"styles": [{"id": "S1", "node_name": "Fallback Style"}]},
        reconstruction_path=None,
    )

    assert payloads[0].centric_style_id == "S1"
    assert payloads[0].style_name == "Fallback Style"


def test_resolve_reconstruction_path_prefers_explicit_then_config_dir(
    tmp_path,
    monkeypatch,
) -> None:
    explicit = tmp_path / "explicit.py"
    config_dir = tmp_path / "centric-config"
    config_dir.mkdir()
    config_path = config_dir / "reconstruction.py"
    config_path.write_text("", encoding="utf-8")
    monkeypatch.setenv("CENTRIC_CONFIG_DIR", str(config_dir))

    assert resolve_reconstruction_path(explicit) == explicit
    assert resolve_reconstruction_path() == config_path
