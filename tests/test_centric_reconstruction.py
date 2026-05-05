import pytest

from centric_mdm_validation.centric.reconstruction import (
    has_private_report_hook,
    has_private_validation_hook,
    inspect_reconstruction_runtime,
    load_private_reconstruction_module,
    project_master_products,
    reconstruct_master_products_from_records,
    reconstruct_target_records,
    report_validation_results,
    resolve_reconstruction_path,
    validate_projected_products,
)


def test_master_reconstruction_and_projection_use_private_hooks(tmp_path) -> None:
    module_path = tmp_path / "reconstruction.py"
    module_path.write_text(
        """
def reconstruct_master_products(records_by_endpoint):
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


def project_reconstructed_products(target, reconstructed_products):
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


def test_projection_hook_can_receive_current_endpoint_records(tmp_path) -> None:
    module_path = tmp_path / "reconstruction.py"
    module_path.write_text(
        """
def project_reconstructed_products(target, reconstructed_products, *, records_by_endpoint=None):
    return [
        {
            "target": target,
            "products": len(list(reconstructed_products)),
            "styles": len(records_by_endpoint["styles"]),
        }
    ]
""",
        encoding="utf-8",
    )

    payloads = project_master_products(
        [{"product_id": "S1"}],
        target="dpp",
        records_by_endpoint={"styles": [{"id": "S1"}, {"id": "S2"}]},
        reconstruction_path=module_path,
    )

    assert payloads == [{"target": "dpp", "products": 1, "styles": 2}]


def test_target_reconstruction_hook_builds_directly_from_endpoint_records(tmp_path) -> None:
    module_path = tmp_path / "reconstruction.py"
    module_path.write_text(
        """
def reconstruct_target_records(target, records_by_endpoint):
    return [
        {
            "target": target,
            "styles": [record["id"] for record in records_by_endpoint["styles"]],
        }
    ]
""",
        encoding="utf-8",
    )

    payloads = reconstruct_target_records(
        "dpp",
        {"styles": [{"id": "S1"}, {"id": "S2"}]},
        reconstruction_path=module_path,
    )

    assert payloads == [{"target": "dpp", "styles": ["S1", "S2"]}]


def test_project_master_products_requires_private_projection_for_dpp(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.chdir(tmp_path)

    products = reconstruct_master_products_from_records(
        {"styles": [{"id": "S1", "node_name": "Fallback Style"}]},
        reconstruction_path=None,
    )

    with pytest.raises(ValueError, match="Private projection required for target 'dpp'"):
        project_master_products(products, target="dpp", reconstruction_path=None)


def test_public_reconstruction_projects_compact_check(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.chdir(tmp_path)

    products = reconstruct_master_products_from_records(
        {"styles": [{"id": "S1", "node_name": "Fallback Style"}]},
        reconstruction_path=None,
    )
    payloads = project_master_products(products, target="check", reconstruction_path=None)

    assert products[0].product_id == "S1"
    assert products[0].graph["placeholder"] is True
    assert products[0].graph["style_id"] == "S1"
    assert payloads[0]["style_id"] == "S1"
    assert payloads[0]["relationship_ids"] == {}
    assert payloads[0]["counts"]["resolved_records"] == {}


def test_inspect_reconstruction_runtime_reports_private_hooks(tmp_path) -> None:
    module_path = tmp_path / "reconstruction.py"
    module_path.write_text(
        """
def reconstruct_master_products(records_by_endpoint):
    return []


def project_reconstructed_products(target, reconstructed_products):
    return []
""",
        encoding="utf-8",
    )

    runtime = inspect_reconstruction_runtime(
        target="packaging",
        reconstruction_path=module_path,
    )

    assert runtime.path == module_path
    assert runtime.master_strategy == "private reconstruction hook"
    assert runtime.projection_strategy == "private project_reconstructed_products hook"


def test_inspect_reconstruction_runtime_reports_public_placeholder(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("CENTRIC_CONFIG_DIR", raising=False)
    monkeypatch.chdir(tmp_path)

    runtime = inspect_reconstruction_runtime(target="check")

    assert runtime.path is None
    assert runtime.master_strategy == "public style-only placeholder"
    assert runtime.projection_strategy == "public compact reconstruction check"


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


def test_private_reconstruction_module_can_import_split_sibling_modules(tmp_path) -> None:
    module_path = tmp_path / "reconstruction.py"
    helpers = tmp_path / "helpers"
    helpers.mkdir()
    (helpers / "__init__.py").write_text("", encoding="utf-8")
    (helpers / "registry.py").write_text(
        """
VALUE = "loaded"
""",
        encoding="utf-8",
    )
    module_path.write_text(
        """
from helpers.registry import VALUE

def marker():
    return VALUE
""",
        encoding="utf-8",
    )

    module = load_private_reconstruction_module(module_path)

    assert module.marker() == "loaded"


def test_private_validation_and_report_hooks_are_resolved(tmp_path) -> None:
    module_path = tmp_path / "reconstruction.py"
    report_dir = tmp_path / "reports"
    module_path.write_text(
        """
from pathlib import Path

def validate_projected_products(target, payloads, *, rules=None):
    return {
        "rule_set_version": f"{target}-rules",
        "total_products": len(list(payloads)),
        "ready_products": 1,
        "readiness_percent": 50.0,
        "results": [],
    }

def report_validation_results(target, validation_result, output_dir):
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    Path(output_dir, f"{target}.txt").write_text(str(validation_result["total_products"]))
""",
        encoding="utf-8",
    )

    assert has_private_validation_hook(reconstruction_path=module_path) is True
    assert has_private_report_hook(reconstruction_path=module_path) is True

    run = validate_projected_products(
        "packaging",
        [{"style_id": "S1"}, {"style_id": "S2"}],
        reconstruction_path=module_path,
    )
    report_validation_results("packaging", run, report_dir, reconstruction_path=module_path)

    assert run["total_products"] == 2
    assert (report_dir / "packaging.txt").read_text(encoding="utf-8") == "2"
