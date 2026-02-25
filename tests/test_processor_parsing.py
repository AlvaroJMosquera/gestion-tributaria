# tests/test_processor_parsing.py
from pathlib import Path
from backend.app.application.processor import FacturaProcessor


def _noop_log(msg: str):
    pass


def test_safe_parse_removes_invalid_xml_chars(tmp_path: Path):
    p = FacturaProcessor(
        carpeta_entrada=tmp_path,
        carpeta_salida=tmp_path,
        log_callback=_noop_log,
        tenant_id="t",
        root_window=None,
    )

    bad_xml = "\x01\x02<Invoice xmlns:cbc='urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2'><cbc:ID>123</cbc:ID></Invoice>"
    node = p.safe_parse(bad_xml)
    # Debe parsear y permitir encontrar el ID
    # Nota: safe_parse usa lxml etree, no ElementTree
    found = node.xpath("//*[local-name()='ID']/text()")
    assert found and found[0] == "123"