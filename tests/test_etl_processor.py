import pytest
from pathlib import Path
from zipfile import ZipFile
import polars as pl
from backend.app.application.processor import FacturaProcessor

def _noop_log(msg: str):
    pass

@pytest.fixture
def processor(tmp_path):
    return FacturaProcessor(
        carpeta_entrada=tmp_path,
        carpeta_salida=tmp_path,
        log_callback=_noop_log,
        tenant_id="test_tenant",
        root_window=None
    )

def test_extracts_metadata_from_invoice(processor):
    xml_path = Path("tests/fixtures/sample_invoice.xml")
    xml_text = xml_path.read_text(encoding="utf-8")
    
    success, df = processor.procesar_xml_text(xml_text, "sample_invoice.xml")
    assert success is True
    assert not df.is_empty()
    
    row = df.row(0, named=True)
    assert row["DocumentID"] == "FV-100"
    assert row["DocumentType"] == "01"
    assert row["CUFE"] == "1234567890abcdef_cufe"
    assert row["SupplierRazónSocial"] == "Proveedor Ficticio S.A.S"
    assert row["SupplierNIT"] == "900123456"
    assert row["Total_IVA"] == 19000.00
    assert row["LM_Total_A_Pagar"] == "119000.00"

def test_extracts_cude_from_credit_note(processor):
    xml_path = Path("tests/fixtures/sample_credit_note.xml")
    xml_text = xml_path.read_text(encoding="utf-8")
    
    success, df = processor.procesar_xml_text(xml_text, "sample_credit_note.xml")
    assert success is True
    
    row = df.row(0, named=True)
    assert row["DocumentID"] == "NC-200"
    assert row["CUDE"] == "fedcba0987654321_cude"

def test_safe_parse_handles_corrupted_xml(processor):
    xml_path = Path("tests/fixtures/sample_corrupt.xml")
    xml_text = xml_path.read_bytes().decode('utf-8', errors='ignore')
    
    success, df = processor.procesar_xml_text(xml_text, "sample_corrupt.xml")
    assert success is True
    row = df.row(0, named=True)
    assert row["DocumentID"] == "BAD-100"

def test_recursive_discovery_finds_zips_and_xmls(processor, tmp_path):
    sub_dir = tmp_path / "subfolder"
    sub_dir.mkdir()
    
    xml_content = Path("tests/fixtures/sample_invoice.xml").read_text(encoding="utf-8")
    (tmp_path / "top_level.xml").write_text(xml_content, encoding="utf-8")
    
    zip_path = tmp_path / "archivos.zip"
    with ZipFile(zip_path, 'w') as zf:
        zf.writestr('zipped_invoice.xml', xml_content)
        
    found_files = list(processor._iter_xmls_from_path(tmp_path))
    filenames = [f[0] for f in found_files]
    
    assert any("top_level.xml" in f for f in filenames)
    assert any("archivos.zip::zipped_invoice.xml" in f for f in filenames)
    assert len(found_files) >= 2
