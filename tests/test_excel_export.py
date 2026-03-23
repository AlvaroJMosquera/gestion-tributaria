import pytest
import polars as pl
import pandas as pd
from pathlib import Path
from backend.app.application.processor import FacturaProcessor

def _noop_log(msg: str):
    pass

@pytest.fixture
def processor(tmp_path):
    return FacturaProcessor(tmp_path, tmp_path, _noop_log, "test_t")

def test_consolidar_para_excel_handles_multiple_dfs(processor):
    df1 = pl.DataFrame({
        "DocumentID": ["F-1"],
        "Total": [100.0],
        "Producto": ["A"]
    })
    df2 = pl.DataFrame({
        "DocumentID": ["F-2"],
        "Total": [200.0],
        "IVA": [38.0]
    })
    
    # Debe consolidar y alinear columnas faltantes con nulls
    df_consolidated = processor._consolidar_para_excel([df1, df2])
    
    assert df_consolidated.height == 2
    assert "Producto" in df_consolidated.columns
    assert "IVA" in df_consolidated.columns
    
    # Comprobar tipos alineados y valores nulos
    assert df_consolidated.filter(pl.col("DocumentID") == "F-1")["IVA"][0] is None
    assert df_consolidated.filter(pl.col("DocumentID") == "F-2")["Producto"][0] is None

def test_generar_resumen_cuatrimestral_agrupa_fechas_correctamente(processor):
    df = pd.DataFrame({
        "IssueDate": ["2026-01-15", "2026-04-10", "2026-05-20"], # C1, C1, C2
        "Base": [1000, 2000, 3000],           # No sumado según lógica
        "IVA": [190, 380, 570],              # Sumado
        "LM_Total_A_Pagar": [1190, 2380, 3570] # Excluido según lógica
    })
    
    resumen = processor._generar_resumen_cuatrimestral(df)
    
    # El resumen es transpuesto donde la columna 0 es 'Concepto...' y el resto los periodos
    assert not resumen.empty
    conceptos = resumen["Concepto (Impuesto / Base)"].tolist()
    assert "IVA" in conceptos
    
    filas_iva = resumen[resumen["Concepto (Impuesto / Base)"] == "IVA"]
    assert filas_iva["2026-C1"].iloc[0] == 570.0  # 190 + 380
    assert filas_iva["2026-C2"].iloc[0] == 570.0
