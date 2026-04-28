from pathlib import Path

from centric_mdm_validation.centric.config import CONFIG_DIR_ENV_VAR
from centric_mdm_validation.centric.mapper import (
    ProjectionMapping,
    load_projection_mapping,
    project_products,
    resolve_projection_mapping_path,
)


def test_project_products_maps_configured_centric_fields() -> None:
    mapping = ProjectionMapping(
        style_global_id_fields=("custom_article_number",),
        style_name_fields=("custom_article_name", "node_name"),
        style_brand_code_fields=("custom_brand_code",),
        style_brand_name_fields=("custom_brand_name",),
        style_lifecycle_fields=("custom_lifecycle_status",),
        season_code_fields=("code",),
        material_composition_fields=("composition", "technical_composition"),
        attribute_fields={
            "ARTICLE_NUMBER": ("custom_article_number",),
            "PRODUCT_AREA": ("custom_product_area",),
            "PRODUCT_GROUP": ("custom_product_group",),
        },
    )
    payloads = project_products(
        {
            "styles": [
                {
                    "id": "C100",
                    "code": "126",
                    "active": True,
                    "node_name": "Fallback Name",
                    "product_type": "C1808",
                    "parent_season": "C200",
                    "custom_article_number": "999888",
                    "custom_article_name": "Mapped Jacket",
                    "custom_brand_code": "BRAND-A",
                    "custom_brand_name": "Brand A",
                    "custom_product_area": "Tops",
                    "custom_product_group": "Jackets",
                    "custom_lifecycle_status": "production",
                    "bom_main_materials": {"0": "C300", "1": "centric:"},
                }
            ],
            "colorways": [
                {
                    "id": "C400",
                    "code": "722 000",
                    "style": "C100",
                    "sys_id": "cee2a4f4-1c1b-409c-bcee-5a3efc82feca",
                }
            ],
            "seasons": [{"id": "C200", "code": "FW26"}],
            "materials": [
                {
                    "id": "C300",
                    "node_name": "Recycled shell fabric",
                    "composition": "90% recycled polyester, 10% elastane",
                }
            ],
        },
        mapping=mapping,
    )

    assert len(payloads) == 1
    payload = payloads[0]
    assert payload.centric_style_id == "C100"
    assert payload.brand_code == "BRAND-A"
    assert payload.brand_name == "Brand A"
    assert payload.global_style_id == "999888"
    assert payload.style_name == "Mapped Jacket"
    assert payload.product_type_code == "C1808"
    assert payload.lifecycle_status == "production"
    assert payload.season == "FW26"
    assert payload.attributes["ARTICLE_NUMBER"] == "999888"
    assert payload.attributes["PRODUCT_AREA"] == "Tops"
    assert payload.attributes["MATERIAL_COMPOSITION"] == "90% recycled polyester, 10% elastane"
    assert payload.attributes["MAIN_MATERIAL_IDS"] == ["C300"]
    assert payload.variants[0].centric_variant_id == "C400"
    assert payload.variants[0].global_variant_id == "cee2a4f4-1c1b-409c-bcee-5a3efc82feca"
    assert payload.variants[0].sku == "722 000"


def test_project_products_does_not_invent_missing_global_ids() -> None:
    payload = project_products({"styles": [{"id": "C1", "node_name": "No Article"}]})[0]

    assert payload.global_style_id is None
    assert payload.product_type_code is None


def test_projection_mapping_path_prefers_explicit_then_config_dir(
    tmp_path,
    monkeypatch,
) -> None:
    explicit = tmp_path / "explicit.yml"
    config_dir = tmp_path / "centric-config"
    mapping_path = config_dir / "field-mapping.yml"
    config_dir.mkdir()
    mapping_path.write_text("style: {}\n", encoding="utf-8")
    monkeypatch.setenv(CONFIG_DIR_ENV_VAR, str(config_dir))

    assert resolve_projection_mapping_path(explicit) == explicit
    assert resolve_projection_mapping_path() == mapping_path


def test_load_projection_mapping_from_file(tmp_path: Path) -> None:
    path = tmp_path / "field-mapping.yml"
    path.write_text(
        """
style:
  global_style_id_fields:
    - custom_article_number
""",
        encoding="utf-8",
    )

    mapping = load_projection_mapping(path)

    assert mapping.style_global_id_fields == ("custom_article_number",)
