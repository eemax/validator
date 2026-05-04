from centric_mdm_validation.centric.reconstruction import (
    project_master_products,
    reconstruct_master_products_from_records,
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


def test_master_reconstruction_and_projection_use_private_hooks(tmp_path) -> None:
    module_path = tmp_path / "reconstruction.py"
    module_path.write_text(
        """
def reconstruct_master_products(records_by_endpoint, *, mapping=None):
    return [
        {
            "product_id": "MASTER-1",
            "style_id": "STYLE-1",
            "brand_code": "BR",
            "season": "SS26",
            "product_type_code": "JACKET",
            "graph": {"private": True},
            "source_refs": [
                {
                    "endpoint": "styles",
                    "record_id": "STYLE-1",
                    "relation_type": "style",
                }
            ],
        }
    ]


def project_reconstructed_products(target, reconstructed_products, *, mapping=None):
    return [
        {
            "centric_style_id": product.product_id,
            "style_name": f"{target}: {product.brand_code}",
        }
        for product in reconstructed_products
    ]
""",
        encoding="utf-8",
    )

    products = reconstruct_master_products_from_records(
        {"styles": [{"id": "STYLE-1"}]},
        reconstruction_path=module_path,
    )
    payloads = project_master_products(products, target="dpp", reconstruction_path=module_path)

    assert products[0].product_id == "MASTER-1"
    assert products[0].source_refs[0].endpoint == "styles"
    assert payloads[0]["centric_style_id"] == "MASTER-1"
    assert payloads[0]["style_name"] == "dpp: BR"


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
