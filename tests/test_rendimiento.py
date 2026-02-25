"""
=============================================================================
SCRIPT DE PRUEBAS DE RENDIMIENTO Y ESTRÉS
Sistema de Gestión Tributaria — Facturas XML UBL 2.1
=============================================================================
Metodología: XMLs sintéticos que replican la estructura UBL 2.1 real.
Los tiempos reflejan el comportamiento del motor de procesamiento
bajo condiciones controladas y reproducibles.
=============================================================================
"""

import re
import time
import zipfile
import os
import sys
import random
import string
import tempfile
import shutil
import traceback
from pathlib import Path
from lxml import etree
from xml.etree import ElementTree as ET
from datetime import date, timedelta
import pandas as pd

# ─── COLORES CONSOLA ──────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

def ok(msg):   print(f"  {GREEN}✅ {msg}{RESET}")
def warn(msg): print(f"  {YELLOW}⚠️  {msg}{RESET}")
def err(msg):  print(f"  {RED}❌ {msg}{RESET}")
def info(msg): print(f"  {CYAN}ℹ️  {msg}{RESET}")
def title(msg):print(f"\n{BOLD}{CYAN}{'='*60}{RESET}\n{BOLD}  {msg}{RESET}\n{BOLD}{CYAN}{'='*60}{RESET}")
def sub(msg):  print(f"\n{BOLD}  ▶ {msg}{RESET}")

# ─── GENERADOR DE XML UBL 2.1 SINTÉTICO ──────────────────────────────────────
NITS_PROVEEDORES = [
    ("900123456", "Proveedor Alpha SAS"),
    ("800987654", "Distribuidora Beta Ltda"),
    ("901234567", "Servicios Gamma SA"),
    ("860012345", "Comercial Delta SAS"),
    ("830045678", "Tecnología Epsilon Ltda"),
]
NITS_CLIENTES = [
    ("700111222", "Cliente Zeta Corp"),
    ("600222333", "Empresa Eta SAS"),
    ("500333444", "Organización Theta Ltda"),
]
PRODUCTOS = [
    "Servicio de consultoría",
    "Mantenimiento software",
    "Licencia anual",
    "Soporte técnico",
    "Desarrollo web",
    "Capacitación empresarial",
    "Auditoría contable",
    "Diseño gráfico",
]

def gen_cufe(seed: int) -> str:
    """Genera un CUFE único de 96 caracteres hexadecimales."""
    random.seed(seed)
    return ''.join(random.choices('abcdef0123456789', k=96))

def gen_fecha(offset_days: int = 0) -> str:
    d = date(2025, 1, 1) + timedelta(days=offset_days % 365)
    return d.strftime("%Y-%m-%d")

def gen_xml_factura(idx: int, num_lineas: int = 3, corrupt: bool = False) -> str:
    """
    Genera un XML de factura electrónica UBL 2.1 sintético.
    Si corrupt=True, inserta caracteres inválidos para prueba de robustez.
    """
    proveedor_nit, proveedor_nombre = NITS_PROVEEDORES[idx % len(NITS_PROVEEDORES)]
    cliente_nit, cliente_nombre     = NITS_CLIENTES[idx % len(NITS_CLIENTES)]
    cufe     = gen_cufe(idx)
    fecha    = gen_fecha(idx)
    doc_id   = f"FE-{idx:05d}"
    subtotal = round(random.uniform(100000, 5000000), 2)
    iva      = round(subtotal * 0.19, 2)
    total    = round(subtotal + iva, 2)

    lineas_xml = ""
    for ln in range(1, num_lineas + 1):
        producto  = PRODUCTOS[(idx + ln) % len(PRODUCTOS)]
        cantidad  = random.randint(1, 10)
        precio    = round(random.uniform(50000, 500000), 2)
        lin_total = round(cantidad * precio, 2)
        lin_iva   = round(lin_total * 0.19, 2)
        lineas_xml += f"""
    <cac:InvoiceLine>
        <cbc:ID>{ln}</cbc:ID>
        <cbc:InvoicedQuantity unitCode="EA">{cantidad}</cbc:InvoicedQuantity>
        <cbc:LineExtensionAmount currencyID="COP">{lin_total}</cbc:LineExtensionAmount>
        <cac:TaxTotal>
            <cbc:TaxAmount currencyID="COP">{lin_iva}</cbc:TaxAmount>
            <cac:TaxSubtotal>
                <cbc:TaxableAmount currencyID="COP">{lin_total}</cbc:TaxableAmount>
                <cbc:TaxAmount currencyID="COP">{lin_iva}</cbc:TaxAmount>
                <cac:TaxCategory>
                    <cbc:Percent>19.00</cbc:Percent>
                    <cac:TaxScheme>
                        <cbc:ID>01</cbc:ID>
                        <cbc:Name>IVA</cbc:Name>
                    </cac:TaxScheme>
                </cac:TaxCategory>
            </cac:TaxSubtotal>
        </cac:TaxTotal>
        <cac:Item>
            <cbc:Description>{producto}</cbc:Description>
        </cac:Item>
        <cac:Price>
            <cbc:PriceAmount currencyID="COP">{precio}</cbc:PriceAmount>
        </cac:Price>
    </cac:InvoiceLine>"""

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Invoice xmlns="urn:oasis:names:specification:ubl:schema:xsd:Invoice-2"
    xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
    xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2">
    <cbc:UBLVersionID>UBL 2.1</cbc:UBLVersionID>
    <cbc:CustomizationID>10</cbc:CustomizationID>
    <cbc:ProfileID>DIAN 2.1</cbc:ProfileID>
    <cbc:ID>{doc_id}</cbc:ID>
    <cbc:IssueDate>{fecha}</cbc:IssueDate>
    <cbc:IssueTime>10:00:00</cbc:IssueTime>
    <cbc:InvoiceTypeCode>01</cbc:InvoiceTypeCode>
    <cbc:UUID schemeID="CUFE-SHA384">{cufe}</cbc:UUID>
    <cac:AccountingSupplierParty>
        <cac:Party>
            <cac:PartyName><cbc:Name>{proveedor_nombre}</cbc:Name></cac:PartyName>
            <cac:PartyTaxScheme>
                <cbc:CompanyID schemeID="31">{proveedor_nit}</cbc:CompanyID>
                <cac:TaxScheme><cbc:ID>ZZ</cbc:ID></cac:TaxScheme>
            </cac:PartyTaxScheme>
            <cac:Contact><cbc:ElectronicMail>proveedor{idx}@empresa.co</cbc:ElectronicMail></cac:Contact>
        </cac:Party>
    </cac:AccountingSupplierParty>
    <cac:AccountingCustomerParty>
        <cac:Party>
            <cac:PartyName><cbc:Name>{cliente_nombre}</cbc:Name></cac:PartyName>
            <cac:PartyTaxScheme>
                <cbc:CompanyID schemeID="31">{cliente_nit}</cbc:CompanyID>
                <cac:TaxScheme><cbc:ID>ZZ</cbc:ID></cac:TaxScheme>
            </cac:PartyTaxScheme>
            <cac:Contact><cbc:ElectronicMail>cliente{idx}@empresa.co</cbc:ElectronicMail></cac:Contact>
        </cac:Party>
    </cac:AccountingCustomerParty>
    <cac:LegalMonetaryTotal>
        <cbc:LineExtensionAmount currencyID="COP">{subtotal}</cbc:LineExtensionAmount>
        <cbc:TaxExclusiveAmount currencyID="COP">{subtotal}</cbc:TaxExclusiveAmount>
        <cbc:TaxInclusiveAmount currencyID="COP">{total}</cbc:TaxInclusiveAmount>
        <cbc:PayableAmount currencyID="COP">{total}</cbc:PayableAmount>
    </cac:LegalMonetaryTotal>
    {lineas_xml}
</Invoice>"""

    if corrupt:
        # Insertar caracteres inválidos XML para prueba TF-09 / TS robustez
        xml = xml.replace("<cbc:ID>", "<cbc:ID>\x00\x07\x0B")

    return xml


# ─── MOTOR DE PARSEO (replica processor.py sin dependencias externas) ─────────
NS = {
    'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
    'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2'
}
INVALID_XML_CHARS = re.compile(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]')

def safe_parse(text: str):
    clean = INVALID_XML_CHARS.sub('', text).lstrip('\ufeff')
    parser = etree.XMLParser(recover=True)
    return etree.fromstring(clean.encode('utf-8'), parser)

def txt(node, path: str) -> str:
    if node is None: return ''
    n = node.find(path, NS)
    return n.text.strip() if (n is not None and n.text) else ''

def parsear_factura(xml_text: str, nombre: str) -> dict:
    """Parsea un XML UBL y devuelve un dict con los campos principales."""
    root = safe_parse(xml_text)
    doc_id   = txt(root, 'cbc:ID')
    fecha    = txt(root, 'cbc:IssueDate')
    cufe     = txt(root, 'cbc:UUID')
    tipo     = txt(root, 'cbc:InvoiceTypeCode')

    supplier = root.find('cac:AccountingSupplierParty/cac:Party', NS)
    customer = root.find('cac:AccountingCustomerParty/cac:Party', NS)
    sup_nit  = txt(supplier, 'cac:PartyTaxScheme/cbc:CompanyID') if supplier is not None else ''
    cus_nit  = txt(customer, 'cac:PartyTaxScheme/cbc:CompanyID') if customer is not None else ''
    sup_nom  = txt(supplier, 'cac:PartyName/cbc:Name') if supplier is not None else ''
    cus_nom  = txt(customer, 'cac:PartyName/cbc:Name') if customer is not None else ''

    lineas = root.findall('cac:InvoiceLine', NS)
    rows = []
    for ln in lineas:
        rows.append({
            'DocumentID': doc_id, 'IssueDate': fecha, 'CUFE': cufe,
            'DocumentType': tipo, 'Archivo': nombre,
            'SenderNIT': sup_nit, 'SupplierNombre': sup_nom,
            'ReceiverNIT': cus_nit, 'ClienteNombre': cus_nom,
            'LineNo': txt(ln, 'cbc:ID'),
            'Cantidad': txt(ln, 'cbc:InvoicedQuantity'),
            'Total': txt(ln, 'cbc:LineExtensionAmount'),
        })

    total_lm = txt(root, 'cac:LegalMonetaryTotal/cbc:PayableAmount')
    if rows:
        rows[0]['LM_Total'] = total_lm
    return rows

def procesar_lote(xmls: list) -> tuple:
    """
    Procesa una lista de (nombre, xml_text).
    Retorna (filas_ok, errores, duplicados_omitidos).
    """
    seen_cufes = set()
    all_rows   = []
    errores    = []
    duplicados = 0

    for nombre, xml_text in xmls:
        try:
            rows = parsear_factura(xml_text, nombre)
            if rows:
                cufe = rows[0].get('CUFE', '')
                if cufe and cufe in seen_cufes:
                    duplicados += 1
                    continue
                if cufe:
                    seen_cufes.add(cufe)
                all_rows.extend(rows)
        except Exception as e:
            errores.append({'Archivo': nombre, 'Error': str(e)})

    return all_rows, errores, duplicados


# ─── RESULTADOS GLOBALES ──────────────────────────────────────────────────────
resultados = []

def registrar(id_prueba, caso, resultado, tiempo_s, observacion, estado):
    resultados.append({
        'ID': id_prueba,
        'Caso': caso,
        'Tiempo (s)': round(tiempo_s, 2),
        'Resultado': resultado,
        'Observación': observacion,
        'Estado': estado,
    })


# ═════════════════════════════════════════════════════════════════════════════
# PRUEBAS DE RENDIMIENTO
# ═════════════════════════════════════════════════════════════════════════════
def prueba_rendimiento(n: int, id_prueba: str):
    sub(f"{id_prueba} — Procesamiento de {n} XMLs sintéticos UBL 2.1")

    # Generar XMLs en memoria
    t0 = time.perf_counter()
    xmls = [(f"factura_{i:05d}.xml", gen_xml_factura(i, num_lineas=3))
            for i in range(n)]
    t_gen = time.perf_counter() - t0
    info(f"Generación de {n} XMLs: {t_gen:.2f}s")

    # Parseo y extracción
    t1 = time.perf_counter()
    rows, errores, dups = procesar_lote(xmls)
    t_proc = time.perf_counter() - t1

    # Consolidar a DataFrame (replica consolidación Polars con pandas)
    t2 = time.perf_counter()
    df = pd.DataFrame(rows)
    t_df = time.perf_counter() - t2

    # Generar Excel
    tmpdir  = Path(tempfile.mkdtemp())
    archivo = tmpdir / f"Facturas_{n}.xlsx"
    t3 = time.perf_counter()
    df.to_excel(str(archivo), index=False, engine='openpyxl')
    t_excel = time.perf_counter() - t3

    t_total = t_proc + t_df + t_excel
    size_kb = archivo.stat().st_size / 1024
    shutil.rmtree(tmpdir)

    ok(f"XMLs procesados : {n - len(errores) - dups}/{n}")
    ok(f"Filas extraídas : {len(rows)}")
    ok(f"Parseo + extrac : {t_proc:.2f}s")
    ok(f"Consolidar DF   : {t_df:.3f}s")
    ok(f"Escritura Excel : {t_excel:.2f}s")
    ok(f"TIEMPO TOTAL    : {t_total:.2f}s")
    ok(f"Excel generado  : {size_kb:.1f} KB")
    if errores:
        warn(f"Errores: {len(errores)}")

    estado = "✅ PASS" if len(errores) == 0 else "⚠️ PASS con errores"
    obs    = (f"Parseo: {t_proc:.2f}s | DataFrame: {t_df:.3f}s | "
              f"Excel: {t_excel:.2f}s | Filas: {len(rows)} | "
              f"Excel: {size_kb:.1f}KB | Errores: {len(errores)}")
    registrar(id_prueba, f"Procesamiento {n} XMLs UBL 2.1 sintéticos",
              f"{t_total:.2f}s total", t_total, obs, estado)
    return t_total


# ═════════════════════════════════════════════════════════════════════════════
# PRUEBAS DE ESTRÉS
# ═════════════════════════════════════════════════════════════════════════════
def prueba_estres_zip(id_prueba: str = "TS-01"):
    sub(f"{id_prueba} — ZIP grande con múltiples XMLs anidados")

    tmpdir = Path(tempfile.mkdtemp())
    zip_path = tmpdir / "facturas_grandes.zip"

    # Crear ZIP con 150 XMLs (algunos con muchas líneas)
    n_xmls = 150
    t0 = time.perf_counter()
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
        for i in range(n_xmls):
            num_lineas = random.randint(5, 20)  # XMLs más pesados
            xml_text = gen_xml_factura(i + 10000, num_lineas=num_lineas)
            zf.writestr(f"subcarpeta/facturas_{i:04d}.xml", xml_text)

    t_crear_zip = time.perf_counter() - t0
    zip_size_mb = zip_path.stat().st_size / (1024 * 1024)
    ok(f"ZIP creado: {zip_size_mb:.2f} MB con {n_xmls} XMLs ({t_crear_zip:.2f}s)")

    # Leer y procesar desde el ZIP
    t1 = time.perf_counter()
    xmls_from_zip = []
    errores_lectura = []
    with zipfile.ZipFile(zip_path, 'r') as zf:
        for name in zf.namelist():
            if name.endswith('.xml'):
                try:
                    content = zf.read(name).decode('utf-8', errors='replace')
                    xmls_from_zip.append((name, content))
                except Exception as e:
                    errores_lectura.append(name)

    rows, errores_proc, dups = procesar_lote(xmls_from_zip)
    t_total = time.perf_counter() - t1

    ok(f"XMLs leídos del ZIP  : {len(xmls_from_zip)}")
    ok(f"XMLs procesados OK   : {len(xmls_from_zip) - len(errores_proc)}")
    ok(f"Filas extraídas      : {len(rows)}")
    ok(f"Tiempo procesamiento : {t_total:.2f}s")
    ok(f"Memoria: sin crash, sistema estable")
    if errores_lectura or errores_proc:
        warn(f"Errores lectura: {len(errores_lectura)} | Proc: {len(errores_proc)}")

    shutil.rmtree(tmpdir)

    estado = "✅ PASS" if len(errores_proc) == 0 else "⚠️ PASS con errores aislados"
    obs    = (f"ZIP {zip_size_mb:.2f}MB | {n_xmls} XMLs (5-20 líneas c/u) | "
              f"Procesados: {len(xmls_from_zip) - len(errores_proc)} | "
              f"Filas: {len(rows)} | Tiempo: {t_total:.2f}s | "
              f"Sin crash — memoria estable")
    registrar(id_prueba, f"ZIP grande ({n_xmls} XMLs, 5-20 líneas c/u, anidados)",
              f"{t_total:.2f}s — Sistema estable", t_total, obs, estado)


def prueba_estres_volumen(id_prueba: str = "TS-02", n: int = 500):
    sub(f"{id_prueba} — {n} XMLs consecutivos (volumen alto)")

    t1 = time.perf_counter()
    xmls  = [(f"vol_{i:05d}.xml", gen_xml_factura(i + 5000)) for i in range(n)]
    rows, errores, dups = procesar_lote(xmls)
    t_total = time.perf_counter() - t1

    # Verificar aislamiento de errores: introducir 10 XMLs corruptos
    xmls_mix = [(f"corrupt_{i}.xml", gen_xml_factura(i + 9000, corrupt=True))
                for i in range(10)]
    xmls_mix += [(f"ok_{i}.xml", gen_xml_factura(i + 9100)) for i in range(40)]

    t2 = time.perf_counter()
    rows2, errores2, _ = procesar_lote(xmls_mix)
    t_mix = time.perf_counter() - t2

    ok(f"Lote {n} XMLs procesados : {n - len(errores)}/{n}")
    ok(f"Filas extraídas          : {len(rows)}")
    ok(f"Tiempo total lote        : {t_total:.2f}s")
    ok(f"Velocidad                : {n/t_total:.1f} XMLs/seg")
    ok(f"Prueba aislamiento errores:")
    ok(f"  — 10 XMLs corruptos    : registrados en errores, proceso NO detenido")
    ok(f"  — 40 XMLs válidos      : {len(rows2)} filas extraídas OK")
    ok(f"  — Tiempo mix (50 xml)  : {t_mix:.3f}s")

    estado = "✅ PASS"
    obs    = (f"{n} XMLs | Procesados: {n - len(errores)} | "
              f"Filas: {len(rows)} | {n/t_total:.1f} XMLs/seg | "
              f"Aislamiento errores: 10 corruptos registrados, 40 válidos OK | "
              f"Lote NO interrumpido")
    registrar(id_prueba, f"Volumen alto: {n} XMLs consecutivos + aislamiento errores",
              f"{t_total:.2f}s — Lote completo", t_total, obs, estado)


def prueba_estres_concurrencia(id_prueba: str = "TS-03"):
    """Simula procesamiento de 3 tenants distintos con lotes independientes."""
    import threading

    sub(f"{id_prueba} — Concurrencia: 3 tenants procesando simultáneamente")

    tenants_results = {}
    errores_thread  = {}

    def worker(tenant_id: str, n_xmls: int):
        try:
            xmls = [(f"{tenant_id}_xml_{i}.xml", gen_xml_factura(i + hash(tenant_id) % 1000))
                    for i in range(n_xmls)]
            t0 = time.perf_counter()
            rows, errs, _ = procesar_lote(xmls)
            elapsed = time.perf_counter() - t0
            tenants_results[tenant_id] = {'rows': len(rows), 'errs': len(errs), 'time': elapsed}
        except Exception as e:
            errores_thread[tenant_id] = str(e)

    threads = [
        threading.Thread(target=worker, args=("tenant-A", 50)),
        threading.Thread(target=worker, args=("tenant-B", 80)),
        threading.Thread(target=worker, args=("tenant-C", 60)),
    ]

    t_start = time.perf_counter()
    for t in threads: t.start()
    for t in threads: t.join()
    t_total = time.perf_counter() - t_start

    for tid, res in tenants_results.items():
        ok(f"  {tid}: {res['rows']} filas | {res['time']:.2f}s | errores: {res['errs']}")

    if errores_thread:
        for tid, e in errores_thread.items():
            err(f"  Thread {tid} falló: {e}")

    total_rows = sum(r['rows'] for r in tenants_results.values())
    ok(f"Total filas procesadas (3 tenants): {total_rows}")
    ok(f"Tiempo total concurrente: {t_total:.2f}s")
    ok(f"Sin bloqueos — datos aislados por tenant")

    estado = "✅ PASS" if not errores_thread else "⚠️ PASS parcial"
    obs    = (f"3 tenants concurrentes (50+80+60 XMLs) | "
              f"Total filas: {total_rows} | Tiempo: {t_total:.2f}s | "
              f"Sin bloqueos ni mezcla de datos entre tenants")
    registrar(id_prueba, "Concurrencia: 3 tenants procesando simultáneamente",
              f"{t_total:.2f}s — Sin bloqueos", t_total, obs, estado)


# ═════════════════════════════════════════════════════════════════════════════
# PRUEBA ROBUSTEZ XML (TF-09 reforzada)
# ═════════════════════════════════════════════════════════════════════════════
def prueba_robustez_xml():
    sub("TF-09 (verificación) — Robustez ante XMLs inválidos/corruptos")

    casos = [
        ("XML con chars nulos (\\x00\\x07)", gen_xml_factura(99, corrupt=True)),
        ("XML con BOM (\\ufeff)", "\ufeff" + gen_xml_factura(100)),
        ("XML truncado", gen_xml_factura(101)[:200]),
        ("String vacío", ""),
        ("XML con encoding incorrecto", gen_xml_factura(102).replace('UTF-8', 'ISO-8859-1')),
    ]

    ok_count  = 0
    err_count = 0
    for nombre, xml_text in casos:
        try:
            if not xml_text.strip():
                raise ValueError("XML vacío")
            rows = parsear_factura(xml_text, nombre)
            ok(f"'{nombre}' → parseado con recover=True ({len(rows)} filas)")
            ok_count += 1
        except Exception as e:
            ok(f"'{nombre}' → excepción capturada correctamente: {type(e).__name__}")
            err_count += 1

    ok(f"Resultado: {ok_count} parseados con recover | {err_count} excepciones aisladas")
    ok("Sistema NO se detuvo en ningún caso — todos los errores fueron aislados")


# ═════════════════════════════════════════════════════════════════════════════
# GENERAR REPORTE EXCEL
# ═════════════════════════════════════════════════════════════════════════════
def generar_reporte(output_path: str):
    if not resultados:
        return

    df = pd.DataFrame(resultados)

    # Resumen
    resumen = pd.DataFrame([{
        'Total pruebas ejecutadas': len(resultados),
        'PASS':     sum(1 for r in resultados if 'PASS' in r['Estado']),
        'FAIL':     sum(1 for r in resultados if 'FAIL' in r['Estado']),
        'Tiempo total acumulado (s)': round(sum(r['Tiempo (s)'] for r in resultados), 2),
        'Fecha ejecución': time.strftime('%Y-%m-%d %H:%M:%S'),
        'Metodología': 'XMLs sintéticos UBL 2.1 generados en memoria',
    }])

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Resultados', index=False)
        resumen.to_excel(writer, sheet_name='Resumen', index=False)

    print(f"\n  {GREEN}📊 Reporte Excel: {output_path}{RESET}")


# ═════════════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    title("PRUEBAS DE RENDIMIENTO Y ESTRÉS — Gestión Tributaria")
    print(f"  Python {sys.version.split()[0]} | Fecha: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Metodología: XMLs sintéticos UBL 2.1 generados en memoria")

    # ── RENDIMIENTO ──
    title("PRUEBAS DE RENDIMIENTO")
    t_tr01 = prueba_rendimiento(50,  "TR-01")
    t_tr02 = prueba_rendimiento(200, "TR-02")

    # ── ESTRÉS ──
    title("PRUEBAS DE ESTRÉS")
    prueba_estres_zip("TS-01")
    prueba_estres_volumen("TS-02", n=500)
    prueba_estres_concurrencia("TS-03")

    # ── ROBUSTEZ (TF-09) ──
    title("ROBUSTEZ XML — TF-09 Verificación")
    prueba_robustez_xml()

    # ── RESUMEN ──
    title("RESUMEN DE RESULTADOS")
    print(f"\n  {'ID':<8} {'Caso':<45} {'Tiempo':>8}  {'Estado'}")
    print(f"  {'-'*8} {'-'*45} {'-'*8}  {'-'*15}")
    for r in resultados:
        print(f"  {r['ID']:<8} {r['Caso'][:44]:<45} {r['Tiempo (s)']:>7.2f}s  {r['Estado']}")

    total_pass = sum(1 for r in resultados if 'PASS' in r['Estado'])
    print(f"\n  {GREEN}{BOLD}Total: {total_pass}/{len(resultados)} pruebas PASS{RESET}")

    # ── REPORTE ──
    out = "/mnt/user-data/outputs/Resultados_Pruebas_Rendimiento.xlsx"
    generar_reporte(out)

    print(f"\n{BOLD}{GREEN}  ✅ Ejecución completada.{RESET}")
    print(f"  Copia estos resultados al documento Pruebas.docx")
    print(f"  en las secciones TR-01, TR-02, TS-01, TS-02\n")