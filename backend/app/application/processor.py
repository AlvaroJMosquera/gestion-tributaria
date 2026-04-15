import re
import xml.etree.ElementTree as ET
from lxml import etree
from pathlib import Path
from zipfile import ZipFile
import polars as pl
import pandas as pd
from tkinter import messagebox
import tempfile
import threading
import os
from backend.app.infrastructure.db.db_repository import save_batch
from backend.app.infrastructure.db.db_dedup import upsert_document_with_lines_idempotent, apply_unique_indexes
from backend.app.infrastructure.db.db_config import get_session, get_engine
from dotenv import load_dotenv
from sqlalchemy import text
from datetime import datetime
from io import BytesIO
from typing import Iterator, Tuple

class FacturaProcessor:
    def __init__(self, carpeta_entrada, carpeta_salida, log_callback, tenant_id, root_window=None):
        self.carpeta_entrada = carpeta_entrada
        self.carpeta_salida = carpeta_salida
        self.log = log_callback
        self.tenant_id = tenant_id
        self.root_window = root_window  # ⭐ Nueva: referencia al root

        # Controladores de estado (Pausar / Cancelar)
        self.pause_event = threading.Event()
        self.cancel_event = threading.Event()

        # Crear carpeta de salida si no existe
        self.carpeta_salida.mkdir(parents=True, exist_ok=True)

        # Namespaces UBL
        self.NS = {
            'cac': 'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
            'cbc': 'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2'
        }

        # Caracteres inválidos en XML
        self.INVALID_XML_CHARS = re.compile(r'[\x00-\x08\x0B-\x0C\x0E-\x1F]')

    def safe_parse(self, text: str) -> etree._Element:
        """Parsea un string XML, eliminando primero caracteres inválidos."""
        clean = self.INVALID_XML_CHARS.sub('', text).lstrip('\ufeff')
        parser = etree.XMLParser(recover=True)
        return etree.fromstring(clean.encode('utf-8'), parser)

    def _dedup_key_from_df(self, df: pl.DataFrame) -> str:
        """Construye una llave estable para deduplicar a nivel Excel."""
        if df.height == 0:
            return ""

        r0 = df.row(0, named=True)

        cufe = (r0.get("CUFE") or "").strip()
        cude = (r0.get("CUDE") or "").strip()
        if cufe:
            return f"CUFE::{cufe}"
        if cude:
            return f"CUDE::{cude}"

        docid = (r0.get("DocumentID") or "").strip()
        issued = (r0.get("IssueDate") or "").strip()
        sender = (r0.get("SenderNIT") or r0.get("SupplierNIT") or "").strip()
        receiver = (r0.get("ReceiverNIT") or r0.get("CustomerNIT") or "").strip()

        # fallback razonable (no perfecto, pero útil)
        return f"FALLBACK::{docid}::{issued}::{sender}::{receiver}"

    def txt(self, node, path: str) -> str:
        """Extrae el texto del nodo dado un path (con namespaces)."""
        if node is None:
            return ''
        n = node.find(path, self.NS)
        return n.text.strip() if (n is not None and n.text) else ''

    def recortar_xml(self, text: str, tag: str) -> str:
        """Toma un string XML y devuelve solo el fragmento comprendido entre <tag> ... </tag>."""
        ini = text.find(f'<{tag}')
        fin = text.rfind(f'</{tag}>') + len(f'</{tag}>')
        return text[ini:fin] if 0 <= ini < fin else text

    def extraer_metadata_invoice(self, root: etree._Element) -> dict:
        md = {
            'DocumentID': self.txt(root, 'cbc:ID'),
            'IssueDate': self.txt(root, 'cbc:IssueDate'),
            'IssueTime': self.txt(root, 'cbc:IssueTime'),
            'CustomizationID': self.txt(root, 'cbc:CustomizationID'),
            'ProfileID': self.txt(root, 'cbc:ProfileID'),
            'Documento Electronico': self.txt(root, 'cbc:ProfileID'),
        }

        tp = (
            self.txt(root, 'cbc:InvoiceTypeCode')
            or self.txt(root, 'cbc:CreditNoteTypeCode')
            or self.txt(root, 'cbc:DebitNoteTypeCode')
            or self.txt(root, 'cbc:DocumentType')
            or self.txt(root, 'cbc:ProfileID')
        )
        md['DocumentType'] = tp
        md['Tipo de Operacion'] = tp

        if root.tag.endswith('CreditNote') or root.tag.endswith('DebitNote'):
            md['ParentDocumentID'] = self.txt(root, 'cac:BillingReference/cac:InvoiceDocumentReference/cbc:ID')
        else:
            md['ParentDocumentID'] = self.txt(root, 'cbc:ParentDocumentID')

        md['Orden de pedido'] = self.txt(root, 'cac:OrderReference/cbc:ID')
        md['Fecha de orden de pedido'] = self.txt(root, 'cac:OrderReference/cbc:IssueDate')
        md['Forma de pago'] = self.txt(root, 'cac:PaymentMeans/cbc:PaymentMeansCode')
        md['Medio de pago'] = self.txt(root, 'cac:PaymentMeans/cbc:PaymentID')
        return md

    def _extraer_md_y_parties_desde_fila(self, fila: dict) -> tuple[dict, dict]:
        md_doc = {
            "DocumentID": fila.get("DocumentID") or fila.get("cbc:ID"),
            "DocumentType": fila.get("DocumentType") or fila.get("Tipo de Operacion") or "Invoice",
            "IssueDate": fila.get("IssueDate"),
            "IssueTime": fila.get("IssueTime"),
            "CUFE": fila.get("CUFE"),
            "CUDE": fila.get("CUDE"),
        }
        for k, v in list(fila.items()):
            if isinstance(k, str) and k.startswith("Total_"):
                md_doc[k] = v
        for k, v in list(fila.items()):
            if isinstance(k, str) and k.startswith("LM_"):
                md_doc[k] = v

        parties = {
            "SupplierRazónSocial": fila.get("SupplierRazónSocial"),
            "SupplierCorreo": fila.get("SupplierCorreo"),
            "SupplierPaís": fila.get("SupplierPaís"),
            "SupplierMunicipio": fila.get("SupplierMunicipio"),
            "SenderNIT": fila.get("SenderNIT") or fila.get("SupplierNIT"),
            "CustomerRazónSocial": fila.get("CustomerRazónSocial"),
            "CustomerCorreo": fila.get("CustomerCorreo"),
            "CustomerPaís": fila.get("CustomerPaís"),
            "CustomerMunicipio": fila.get("CustomerMunicipio"),
            "ReceiverNIT": fila.get("ReceiverNIT") or fila.get("CustomerNIT"),
        }
        return md_doc, parties

    def parse_float(self, val: str | float | int | None) -> float:
        if val is None or val == "":
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)

        s = str(val).strip()
        if ',' in s and '.' in s:
            if s.rfind(',') > s.rfind('.'):
                s = s.replace('.', '').replace(',', '.')
            else:
                s = s.replace(',', '')
        elif ',' in s:
            # Si hay múltiples comas, son miles
            if s.count(',') > 1:
                s = s.replace(',', '')
            # Si hay una coma y 3 dígitos después, asumimos miles si no obedece el patrón de céntimos
            elif len(s) - s.rfind(',') == 4:
                s = s.replace(',', '')
            else:
                s = s.replace(',', '.')

        try:
            return float(s)
        except ValueError:
            return 0.0

    def _build_line_dict_from_row(self, fila: dict) -> dict:
        out = {
            "SKU": fila.get("SKU", ""),
            "Producto": fila.get("Producto", ""),
            "Cantidad": fila.get("Cantidad", 0),
            "Base": fila.get("Base", 0),
            "Descuento": fila.get("Descuento", 0),
            "Total": fila.get("Total", None),
        }
        if "IVA" in fila:
            out["IVA"] = fila.get("IVA")
        if "IVA (%)" in fila:
            out["IVA (%)"] = fila.get("IVA (%)")

        # ✅ asegurar el código DIAN del IVA para que la persistencia use tax_code
        if "__code__IVA" in fila:
            out["__code__IVA"] = fila.get("__code__IVA")

        fixed = {"SKU", "Producto", "Cantidad", "Valor Unitario", "Base", "Descuento", "Total", "IVA", "IVA (%)"}
        for k, v in fila.items():
            if not isinstance(k, str):
                continue
            if k in fixed:
                continue
            if k.startswith("__code__"):
                continue
            if k.endswith(" (%)"):
                continue
            pct_key = f"{k} (%)"
            if pct_key in fila:
                out[k] = v
                out[pct_key] = fila.get(pct_key)
                code_key = f"__code__{k}"
                if code_key in fila:
                    out[code_key] = fila.get(code_key)
        return out

    def persistir_df_en_bd(self, session, batch_id: int, df_doc: pl.DataFrame, tenant_id: str, file_name: str):
        self.log(f"Iniciando persistencia para {file_name} con tenant_id: {tenant_id}")
        if df_doc.height == 0:
            return

        fila0 = df_doc.row(0, named=True)
        md_doc, parties = self._extraer_md_y_parties_desde_fila(fila0)

        lineas = []
        for r in df_doc.iter_rows(named=True):
            lineas.append(self._build_line_dict_from_row(r))

        upsert_document_with_lines_idempotent(
            session=session,
            batch_id=batch_id,
            md_doc=md_doc,
            parties=parties,
            lines=lineas,
            file_name=file_name,
            tenant_id=tenant_id
        )
        self.log(f"Persistencia completada para {file_name}")

    def extraer_partes(self, root: etree._Element) -> dict:
        datos = {}
        for role, tag in [('Supplier', 'AccountingSupplierParty'), ('Customer', 'AccountingCustomerParty')]:
            nodo = root.find(f'cac:{tag}', self.NS)
            if nodo is None:
                continue
            party = nodo.find('cac:Party', self.NS)
            if party is None: continue
            
            datos[f'{role}RazónSocial'] = self.txt(party, 'cac:PartyLegalEntity/cbc:RegistrationName') or self.txt(party, 'cac:PartyName/cbc:Name')
            datos[f'{role}NombreComercial'] = self.txt(party, 'cac:PartyName/cbc:Name')
            
            nit_val = self.txt(party, 'cac:PartyTaxScheme/cbc:CompanyID') or self.txt(party, 'cac:PartyIdentification/cbc:ID')
            datos[f'{role}NIT'] = nit_val
            
            datos[f'{role}TipoContribuyente'] = self.txt(party, 'cac:PartyTaxScheme/cbc:RegistrationName')
            datos[f'{role}RégimenFiscal'] = self.txt(party, 'cac:PartyTaxScheme/cac:TaxScheme/cbc:ID')
            datos[f'{role}ResponsabilidadTributaria'] = self.txt(party, 'cac:PartyTaxScheme/cac:TaxScheme/cbc:Name')
            datos[f'{role}ActividadEconomica'] = self.txt(party, 'cac:PartyTaxScheme/cbc:TaxLevelCode')

            addr = party.find('cac:PostalAddress', self.NS) or party.find('cac:PhysicalLocation/cac:Address', self.NS)
            if addr is not None:
                street = ' '.join(filter(None, [
                    self.txt(addr, 'cbc:StreetName'),
                    self.txt(addr, 'cbc:BuildingNumber'),
                    self.txt(addr, 'cac:AddressLine/cbc:Line')
                ]))
                datos[f'{role}Dirección'] = street
                datos[f'{role}País'] = self.txt(addr, 'cac:Country/cbc:IdentificationCode')
                datos[f'{role}Departamento'] = self.txt(addr, 'cbc:CountrySubentity')
                datos[f'{role}Municipio'] = self.txt(addr, 'cbc:CityName')
            else:
                datos[f'{role}Dirección'] = ''
                datos[f'{role}País'] = ''
                datos[f'{role}Departamento'] = ''
                datos[f'{role}Municipio'] = ''

            contact = party.find('cac:Contact', self.NS)
            datos[f'{role}Teléfono'] = self.txt(contact, 'cbc:Telephone')
            datos[f'{role}Correo'] = self.txt(contact, 'cbc:ElectronicMail')
        return datos

    def extraer_cufe_cude(self, root: etree._Element) -> tuple[str, str]:
        NS = self.NS
        cufe, cude = "", ""
        tag_local = etree.QName(root.tag).localname

        try:
            uuids_root = root.xpath('./cbc:UUID', namespaces=NS)
            for u in uuids_root:
                scheme = (u.get('schemeName') or '').upper()
                val = (u.text or '').strip()
                if not val:
                    continue
                if 'CUDE' in scheme:
                    cude = cude or val
                elif 'CUFE' in scheme:
                    cufe = cufe or val
                elif 'SHA384' in scheme:
                    if tag_local == 'Invoice':
                        cufe = cufe or val
                    else:
                        cude = cude or val
        except Exception:
            pass

        if cufe and cude:
            return cufe, cude

        try:
            uuid_pdlr = root.xpath(
                './/cac:ParentDocumentLineReference/cac:DocumentReference/cbc:UUID',
                namespaces=NS
            )
            for u in uuid_pdlr:
                scheme = (u.get('schemeName') or '').upper()
                val = (u.text or '').strip()
                if not val:
                    continue
                if 'CUDE' in scheme:
                    cude = cude or val
                elif 'CUFE' in scheme:
                    cufe = cufe or val
                elif 'SHA384' in scheme:
                    if not cufe:
                        cufe = val
                    elif not cude:
                        cude = val
        except Exception:
            pass

        if cufe and cude:
            return cufe, cude

        try:
            desc_nodes = root.xpath(
                './/cac:Attachment/cac:ExternalReference/cbc:Description',
                namespaces=NS
            )
            for dn in desc_nodes:
                if not dn.text:
                    continue
                try:
                    emb = self.safe_parse(dn.text.strip())
                except Exception:
                    continue

                uuids_emb = emb.xpath('.//cbc:UUID', namespaces=NS)
                xml_str = etree.tostring(emb, encoding='unicode')
                for u in uuids_emb:
                    scheme = (u.get('schemeName') or '').upper()
                    val = (u.text or '').strip()
                    if not val:
                        continue
                    if 'CUDE' in scheme:
                        if not cude:
                            cude = val
                    elif 'CUFE' in scheme:
                        if not cufe:
                            cufe = val
                    elif 'SHA384' in scheme:
                        if 'cbc:ID' in xml_str and not cufe:
                            cufe = val
                        elif not cude:
                            cude = val
        except Exception:
            pass

        return cufe, cude

    def extraer_taxes_document(self, root: etree._Element) -> dict:
        taxes = {}
        for tt in root.findall('cac:TaxTotal', self.NS):
            for ts in tt.findall('cac:TaxSubtotal', self.NS):
                tax_id = self.txt(ts, 'cac:TaxCategory/cac:TaxScheme/cbc:ID')
                tax_name = self.txt(ts, 'cac:TaxCategory/cac:TaxScheme/cbc:Name')
                amount = float(self.txt(ts, 'cbc:TaxAmount') or 0)

                pct_raw = self.txt(ts, 'cac:TaxCategory/cbc:Percent')
                try:
                    pct = float(pct_raw) / 100 if pct_raw else None
                except ValueError:
                    pct = None

                name = tax_name if tax_name else (f'Tax_{tax_id}' if tax_id else 'Unknown')
                taxes[f'Total_{name}'] = amount
                taxes[f'Total_{name} (%)'] = pct
                if tax_id:
                    taxes[f'Total_{name}__code'] = self._norm_code(tax_id)
        return taxes
    
    def _norm_code(self, val: str) -> str | None:
        if val is None:
            return None
        s = str(val).strip()
        only_digits = ''.join(ch for ch in s if ch.isdigit())
        only_digits = only_digits.lstrip('0')
        return only_digits or None

    def extraer_lineas(self, xml_string: str) -> list:
        if '<Invoice' in xml_string:
            tag, line_tag, qty_tag = 'Invoice', 'InvoiceLine', 'cbc:InvoicedQuantity'
        elif '<CreditNote' in xml_string:
            tag, line_tag, qty_tag = 'CreditNote', 'CreditNoteLine', 'cbc:CreditedQuantity'
        elif '<DebitNote' in xml_string:
            tag, line_tag, qty_tag = 'DebitNote', 'DebitNoteLine', 'cbc:DebitedQuantity'
        else:
            return []

        bloque = self.recortar_xml(xml_string, tag)
        root = self.safe_parse(bloque)
        filas = []

        for ln in root.findall(f'.//cac:{line_tag}', self.NS):
            prod = ln.find('.//cac:Item/cbc:Description', self.NS)
            sku = (
                self.txt(ln, './/cac:Item/cac:SellersItemIdentification/cbc:ID') or
                self.txt(ln, './/cac:Item/cac:StandardItemIdentification/cbc:ID') or
                self.txt(ln, './/cac:Item/cac:BuyersItemIdentification/cbc:ID')
            )
            qty = self.parse_float(self.txt(ln, qty_tag))
            pu = self.parse_float(self.txt(ln, './/cac:Price/cbc:PriceAmount'))
            base = self.parse_float(self.txt(ln, 'cbc:LineExtensionAmount'))
            dsc = self.parse_float(self.txt(ln, './/cac:AllowanceCharge/cbc:Amount'))

            fila = {
                'SKU': sku,
                'Producto': prod.text.strip() if (prod is not None and prod.text) else '',
                'Cantidad': qty,
                'Valor Unitario': pu,
                'Base': base,
                'Descuento': dsc,
            }

            # ✅ IMPORTANTE: acumular impuestos para evitar que IVA (0%) pise IVA (19%)
            sum_taxes = 0.0

            for ts in ln.findall('.//cac:TaxSubtotal', self.NS):
                tax_id = self.txt(ts, 'cac:TaxCategory/cac:TaxScheme/cbc:ID')
                tax_name = self.txt(ts, 'cac:TaxCategory/cac:TaxScheme/cbc:Name')
                amount = self.parse_float(self.txt(ts, 'cbc:TaxAmount'))

                pct_raw = self.txt(ts, 'cac:TaxCategory/cbc:Percent')
                pct = self.parse_float(pct_raw) if pct_raw else None

                name = tax_name if tax_name else (f'Tax_{tax_id}' if tax_id else 'Unknown')

                # ✅ 1) Total acumulado por impuesto (IVA total, ICA total, etc.)
                fila[name] = float(fila.get(name, 0) or 0) + amount

                # Guardar code DIAN del impuesto si aplica (ej: IVA tax_id=01)
                if tax_id and f'__code__{name}' not in fila:
                    fila[f'__code__{name}'] = self._norm_code(tax_id)

                # ✅ 2) Desglose por tarifa (IVA (19%), IVA (0%)) para AIU y casos mixtos
                if pct is not None:
                    name_rate = f"{name} ({pct:g}%)"   # "IVA (19%)"
                    fila[name_rate] = float(fila.get(name_rate, 0) or 0) + amount

                    # Mantener una columna de tasa (en fracción) por consistencia con tu modelo
                    fila[f"{name_rate} (%)"] = pct / 100

                    # code por tarifa, por si lo quieres diferenciar
                    if tax_id:
                        fila[f"__code__{name_rate}"] = self._norm_code(tax_id)

                # ✅ Acumular para el total de la línea
                sum_taxes += amount

            # Total de línea: Base + impuestos - descuento
            fila['Total'] = base + sum_taxes - dsc
            filas.append(fila)

        return filas

    def procesar_attached_document(self, xml_text: str, nombre_archivo: str):
        bloque_ad = self.recortar_xml(xml_text, 'AttachedDocument')
        root_ad = self.safe_parse(bloque_ad)
        descs = root_ad.findall('.//cac:Attachment/cac:ExternalReference/cbc:Description', self.NS)

        if not descs:
            return False, pl.DataFrame([])

        xml_main = descs[0].text or ''

        if '<Invoice' in xml_main:
            tag = 'Invoice'
        elif '<CreditNote' in xml_main:
            tag = 'CreditNote'
        elif '<DebitNote' in xml_main:
            tag = 'DebitNote'
        else:
            return False, pl.DataFrame([])

        root_main = self.safe_parse(self.recortar_xml(xml_main, tag))

        md_doc = self.extraer_metadata_invoice(root_main)
        cufe, cude = self.extraer_cufe_cude(root_ad)
        taxes_doc = self.extraer_taxes_document(root_main)

        filas = self.extraer_lineas(xml_main)
        if not filas:
            return False, pl.DataFrame([])

        md_part = self.extraer_partes(root_main)

        sender_nodo = root_ad.find('.//cac:SenderParty/cac:PartyTaxScheme/cbc:CompanyID', self.NS)
        md_part['SenderNIT'] = sender_nodo.text.strip() if (sender_nodo is not None and sender_nodo.text) else md_part.get('SupplierNIT', '')

        receiver_nodo = root_ad.find('.//cac:ReceiverParty/cac:PartyTaxScheme/cbc:CompanyID', self.NS)
        md_part['ReceiverNIT'] = receiver_nodo.text.strip() if (receiver_nodo is not None and receiver_nodo.text) else md_part.get('CustomerNIT', '')

        totales = self.extraer_totales_factura(root_main)
        md_all = {**md_doc, **md_part, **taxes_doc, **totales, 'CUFE': cufe, 'CUDE': cude, 'Archivo XML': nombre_archivo}

        df = pl.DataFrame(filas)
        df = df.with_columns([pl.lit(v).alias(k) for k, v in md_all.items()])
        return True, df

    def procesar_xml_text(self, xml_text: str, nombre_archivo: str) -> tuple[bool, pl.DataFrame]:
        raw = xml_text

        if '<AttachedDocument' in raw:
            return self.procesar_attached_document(raw, nombre_archivo)

        if '<Invoice' in raw or '<CreditNote' in raw or '<DebitNote' in raw:
            if '<Invoice' in raw:
                tag = 'Invoice'
            elif '<CreditNote' in raw:
                tag = 'CreditNote'
            else:
                tag = 'DebitNote'

            bloque_xml = self.recortar_xml(raw, tag)
            root = self.safe_parse(bloque_xml)

            filas = self.extraer_lineas(raw)
            if not filas:
                return False, pl.DataFrame([])

            md_doc = self.extraer_metadata_invoice(root)
            cufe, cude = self.extraer_cufe_cude(root)
            taxes_doc = self.extraer_taxes_document(root)
            md_part = self.extraer_partes(root)
            md_part['SenderNIT'] = md_part.get('SupplierNIT', '')
            md_part['ReceiverNIT'] = md_part.get('CustomerNIT', '')

            totales = self.extraer_totales_factura(root)
            md_all = {**md_doc, **md_part, **taxes_doc, **totales, 'CUFE': cufe, 'CUDE': cude, 'Archivo XML': nombre_archivo}

            df = pl.DataFrame(filas)
            df = df.with_columns([pl.lit(v).alias(k) for k, v in md_all.items()])
            return True, df

        return False, pl.DataFrame([])

    def _iter_xmls_from_zipfile(self, zf: ZipFile, prefix: str, depth: int, max_depth: int) -> Iterator[Tuple[str, str]]:
        for nm in zf.namelist():
            low = nm.lower()
            try:
                if low.endswith('.xml'):
                    xml_bytes = zf.read(nm)
                    yield (f"{prefix}::{nm}", xml_bytes.decode('utf-8', errors='ignore'))
                elif low.endswith('.zip') and depth < max_depth:
                    data = zf.read(nm)
                    with ZipFile(BytesIO(data)) as inner:
                        inner_prefix = f"{prefix}::{nm}"
                        yield from self._iter_xmls_from_zipfile(inner, inner_prefix, depth + 1, max_depth)
            except Exception as e:
                self.log(f"  Error leyendo entrada ZIP {prefix}::{nm}: {e}")

    def _iter_xmls_from_path(self, path: Path, max_depth: int = 3) -> Iterator[Tuple[str, str]]:
        if path.is_file():
            low = path.name.lower()
            if low.endswith('.xml'):
                try:
                    yield (path.name, path.read_text(encoding='utf-8', errors='ignore'))
                except Exception as e:
                    self.log(f"Error leyendo {path.name}: {e}")
            elif low.endswith('.zip'):
                try:
                    with ZipFile(path, 'r') as zf:
                        self.log(f"{path.name} contiene {len(zf.namelist())} entradas")
                        yield from self._iter_xmls_from_zipfile(zf, path.name, depth=1, max_depth=max_depth)
                except Exception as e:
                    self.log(f"Error procesando ZIP {path.name}: {e}")
        else:
            for p in path.rglob('*'):
                if p.is_file():
                    low = p.name.lower()
                    if low.endswith('.xml'):
                        try:
                            yield (str(p.relative_to(self.carpeta_entrada)), p.read_text(encoding='utf-8', errors='ignore'))
                        except Exception as e:
                            self.log(f"Error leyendo {p}: {e}")
                    elif low.endswith('.zip'):
                        try:
                            with ZipFile(p, 'r') as zf:
                                self.log(f"{p} contiene {len(zf.namelist())} entradas")
                                yield from self._iter_xmls_from_zipfile(
                                    zf,
                                    str(p.relative_to(self.carpeta_entrada)),
                                    depth=1,
                                    max_depth=max_depth
                                )
                        except Exception as e:
                            self.log(f"Error procesando ZIP {p}: {e}")
                            
    def extraer_totales_factura(self, root: etree._Element) -> dict:
        def txt_from(parent, rel_path: str) -> str:
            if parent is None:
                return ""
            n = parent.find(rel_path, self.NS)
            return n.text.strip() if (n is not None and n.text) else ""

        # 1) Detectar el contenedor correcto según el documento
        tag_local = etree.QName(root.tag).localname  # Invoice | CreditNote | DebitNote | AttachedDocument...

        monetary = None

        # Caso esperado por tipo
        if tag_local == "DebitNote":
            monetary = root.find("cac:RequestedMonetaryTotal", self.NS)
        else:
            monetary = root.find("cac:LegalMonetaryTotal", self.NS)

        # 2) Fallback si el anterior no existe (algunos XML vienen raros o embebidos)
        if monetary is None:
            monetary = (
                root.find(".//cac:LegalMonetaryTotal", self.NS)
                or root.find(".//cac:RequestedMonetaryTotal", self.NS)
            )

        tot = {}

        # 3) Extraer campos (si no existen, quedan "")
        tot["LM_Subtotal_Sin_Impuestos"] = self.parse_float(txt_from(monetary, "cbc:LineExtensionAmount"))
        tot["LM_Descuentos"] = self.parse_float(txt_from(monetary, "cbc:AllowanceTotalAmount"))

        # Algunos documentos incluyen TaxExclusiveAmount (útil para auditoría)
        tot["LM_Total_Sin_Impuestos"] = self.parse_float(txt_from(monetary, "cbc:TaxExclusiveAmount"))

        tot["LM_Total_Con_Impuestos"] = self.parse_float(txt_from(monetary, "cbc:TaxInclusiveAmount"))
        tot["LM_Total_A_Pagar"] = self.parse_float(txt_from(monetary, "cbc:PayableAmount"))

        # Opcionales útiles (no rompen nada si no existen)
        tot["LM_Cargos"] = self.parse_float(txt_from(monetary, "cbc:ChargeTotalAmount"))
        tot["LM_Redondeo"] = self.parse_float(txt_from(monetary, "cbc:PayableRoundingAmount"))

        # 4) Fallback extra: si PayableAmount no existe en el contenedor,
        # buscarlo en cualquier parte (hay notas débito que lo ponen distinto)
        if not tot["LM_Total_A_Pagar"]:
            tot["LM_Total_A_Pagar"] = self.parse_float(self.txt(root, ".//cbc:PayableAmount"))

        # 5) Si TaxInclusiveAmount viene vacío, intenta recuperarlo global
        if not tot["LM_Total_Con_Impuestos"]:
            tot["LM_Total_Con_Impuestos"] = self.parse_float(self.txt(root, ".//cbc:TaxInclusiveAmount"))

        return tot

    def _consolidar_para_excel(self, dfs_norm: list[pl.DataFrame]) -> pl.DataFrame:
        if not dfs_norm:
            return pl.DataFrame([])

        all_cols = sorted({
            c for df in dfs_norm for c in df.columns 
            if not (isinstance(c, str) and c.startswith('Total_'))
        })

        numeric_like = set()
        for df in dfs_norm:
            for c in df.columns:
                if isinstance(c, str) and c.startswith('Total_'):
                    continue
                dt = df.schema.get(c)
                if dt is not None and str(dt) in {"Float64", "Int64", "UInt64", "Decimal"}:
                    numeric_like.add(c)

        for c in all_cols:
            if isinstance(c, str):
                if c.endswith("(%)"):
                    numeric_like.add(c)
        numeric_like.update({"Cantidad", "Base", "Descuento", "Total", "IVA"})

        dfs_sel = []
        for df in dfs_norm:
            add = []
            for c in all_cols:
                if c not in df.columns:
                    if c in numeric_like:
                        add.append(pl.lit(None).cast(pl.Float64).alias(c))
                    else:
                        add.append(pl.lit(None).cast(pl.Utf8).alias(c))
            if add:
                df = df.with_columns(add)

            casted = []
            for c in all_cols:
                if c in numeric_like and str(df.schema[c]) == "Null":
                    casted.append(pl.col(c).cast(pl.Float64))
            if casted:
                df = df.with_columns(casted)

            dfs_sel.append(df.select(all_cols))

        df_total = pl.concat(dfs_sel, how="vertical_relaxed")

        for c in sorted(numeric_like):
            if c in df_total.columns:
                df_total = df_total.with_columns(pl.col(c).cast(pl.Float64, strict=False))

        fix_null = []
        for c, t in df_total.schema.items():
            if c in numeric_like and str(t) == "Null":
                fix_null.append(pl.col(c).cast(pl.Float64))
        if fix_null:
            df_total = df_total.with_columns(fix_null)

        return df_total

    def _generar_resumen_cuatrimestral(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty or 'IssueDate' not in df.columns:
            return pd.DataFrame()

        df_calc = df.copy()
        df_calc['IssueDate_dt'] = pd.to_datetime(df_calc['IssueDate'], errors='coerce')
        df_calc = df_calc.dropna(subset=['IssueDate_dt'])

        if df_calc.empty:
            return pd.DataFrame()

        df_calc['Año'] = df_calc['IssueDate_dt'].dt.year
        df_calc['Mes'] = df_calc['IssueDate_dt'].dt.month
        df_calc['Cuatrimestre'] = ((df_calc['Mes'] - 1) // 4) + 1
        df_calc['Periodo'] = df_calc['Año'].astype(str) + "-C" + df_calc['Cuatrimestre'].astype(str)

        # Identificar qué columnas sumar (Base, impuestos, total por linea, etc.)
        cols_a_sumar = []
        for c in df_calc.columns:
            if not pd.api.types.is_numeric_dtype(df_calc[c]):
                continue
            if c in ['Año', 'Mes', 'Cuatrimestre', 'Cantidad', 'Valor Unitario', 'Descuento', 'Base']:
                continue
            if str(c).startswith('LM_') or str(c).startswith('Total_') or str(c).startswith('__code__'):
                continue
            if str(c).endswith(' (%)'):
                continue
            cols_a_sumar.append(c)

        # Restar los montos si es una nota crédito
        if 'DocumentType' in df_calc.columns and cols_a_sumar:
            es_nc = df_calc['DocumentType'].astype(str).str.lower().str.contains('credit')
            for c in cols_a_sumar:
                df_calc.loc[es_nc, c] = df_calc.loc[es_nc, c].astype(float) * -1

        if cols_a_sumar:
            # Agrupar por Periodo (ej: 2024-C1, 2024-C2)
            resumen = df_calc.groupby('Periodo')[cols_a_sumar].sum(numeric_only=True).reset_index()
            # Formato transpuesto para que "Concepto" sean filas
            resumen_t = resumen.set_index('Periodo').T
            resumen_t.index.name = 'Concepto (Impuesto / Base)'
            resumen_t = resumen_t.reset_index()
            
            # Limpiar filas donde todos los cuatrimestres sean 0
            val_cols = resumen_t.columns[1:]
            resumen_t = resumen_t[(resumen_t[val_cols].abs().sum(axis=1)) > 0.01].copy()
            
            return resumen_t

        return pd.DataFrame()

    def _generar_resumen_proveedores(self, df: pd.DataFrame, is_clientes: bool = False) -> pd.DataFrame:
        nit_col = 'ReceiverNIT' if is_clientes else 'SenderNIT'
        name_col = 'CustomerRazónSocial' if is_clientes else 'SupplierRazónSocial'
        
        if df.empty or 'IssueDate' not in df.columns or nit_col not in df.columns:
            return pd.DataFrame()

        df_calc = df.copy()
        
        # Opcional: Si el usuario quiere ver "Facturas Mías = Ventas", normalmente en Colombia el archivo de venta
        # tiene 'SenderNIT' igual al NIT del usuario que extrae. 
        # Aquí agruparemos asumiendo la dirección que nos piden: 
        # Si es_clientes = True -> Queremos agrupar por el NIT del Cliente (ReceiverNIT)
        # Si es_clientes = False -> Queremos agrupar por el NIT del Proveedor (SenderNIT)
        
        df_calc['IssueDate_dt'] = pd.to_datetime(df_calc['IssueDate'], errors='coerce')
        df_calc = df_calc.dropna(subset=['IssueDate_dt'])

        if df_calc.empty:
            return pd.DataFrame()

        df_calc['Año'] = df_calc['IssueDate_dt'].dt.year
        df_calc['Mes'] = df_calc['IssueDate_dt'].dt.month
        df_calc['Cuatrimestre'] = ((df_calc['Mes'] - 1) // 4) + 1
        df_calc['Periodo'] = df_calc['Año'].astype(str) + "-C" + df_calc['Cuatrimestre'].astype(str)

        if 'DocumentType' in df_calc.columns and 'Total' in df_calc.columns:
            es_nc = df_calc['DocumentType'].astype(str).str.lower().str.contains('credit')
            df_calc.loc[es_nc, 'Total'] = df_calc.loc[es_nc, 'Total'].astype(float) * -1

        df_calc[nit_col] = df_calc[nit_col].fillna('Sin NIT')
        
        if name_col in df_calc.columns:
            df_calc[name_col] = df_calc[name_col].fillna('Sin Razón Social')
            agrupacion = ['Periodo', nit_col, name_col]
        else:
            agrupacion = ['Periodo', nit_col]

        if 'Total' in df_calc.columns:
            resumen = df_calc.groupby(agrupacion)['Total'].sum(numeric_only=True).reset_index()
            if name_col in df_calc.columns:
                resumen_pivot = resumen.pivot_table(
                    index=[nit_col, name_col], 
                    columns='Periodo', 
                    values='Total', 
                    aggfunc='sum',
                    fill_value=0
                ).reset_index()
            else:
                resumen_pivot = resumen.pivot_table(
                    index=[nit_col], 
                    columns='Periodo', 
                    values='Total', 
                    aggfunc='sum',
                    fill_value=0
                ).reset_index()

            val_cols = [c for c in resumen_pivot.columns if c not in [nit_col, name_col]]
            if val_cols:
                resumen_pivot['Acumulado_Total'] = resumen_pivot[val_cols].sum(axis=1)
                resumen_pivot = resumen_pivot.sort_values(by='Acumulado_Total', ascending=False).drop(columns=['Acumulado_Total'])

            return resumen_pivot

        return pd.DataFrame()

    def _show_messagebox(self, msgtype: str, title: str, message: str):
        """Muestra messagebox thread-safe usando root.after()"""
        if self.root_window and self.root_window.winfo_exists():
            if msgtype == "error":
                self.root_window.after(0, lambda: messagebox.showerror(title, message))
            elif msgtype == "info":
                self.root_window.after(0, lambda: messagebox.showinfo(title, message))
            elif msgtype == "warning":
                self.root_window.after(0, lambda: messagebox.showwarning(title, message))

    def _verificar_pausa_y_cancelacion(self) -> bool:
        """Pausa el hilo si está en pausa, o retorna True si debe cancelarse."""
        if self.cancel_event.is_set():
            return True
            
        if self.pause_event.is_set():
            self.log("⏸️ Proceso pausado. Esperando para reanudar...")
            while self.pause_event.is_set():
                if self.cancel_event.is_set():
                    return True
                import time
                time.sleep(0.5)
            self.log("▶️ Proceso reanudado.")
            
        return False

    def ejecutar_proceso(self, persist_db: bool = True):
        # Reiniciar eventos al iniciar
        self.pause_event.clear()
        self.cancel_event.clear()
        
        load_dotenv()

        self.log("Iniciando procesamiento de facturas XML...")
        self.log(f"Carpeta de entrada: {self.carpeta_entrada}")
        self.log(f"Carpeta de salida: {self.carpeta_salida}")

        all_dfs: list[tuple[str, pl.DataFrame]] = []
        errores = []
        archivos_procesados = 0

        # ✅ DEDUP Excel
        seen_keys = set()
        duplicados = []

        encontrados = 0
        for nombre_logico, xml_text in self._iter_xmls_from_path(self.carpeta_entrada, max_depth=5):
            if self._verificar_pausa_y_cancelacion():
                self.log("🛑 Proceso cancelado por el usuario (Fase A).")
                self._show_messagebox("warning", "Cancelado", "El procesamiento fue cancelado por el usuario.")
                return

            encontrados += 1
            self.log(f"Procesando: {nombre_logico}")
            try:
                ok, df = self.procesar_xml_text(xml_text, nombre_logico)
                if ok:
                    # ✅ clave de deduplicación (CUFE/CUDE > fallback)
                    key = self._dedup_key_from_df(df)

                    if key and key in seen_keys:
                        duplicados.append({"Archivo XML": nombre_logico, "Estado": f"Duplicado omitido (key={key})"})
                        self.log(f"  ⚠ Duplicado omitido en Excel: {nombre_logico} ({key})")
                        continue

                    if key:
                        seen_keys.add(key)

                    all_dfs.append((nombre_logico, df))
                    archivos_procesados += 1
                    self.log("  Procesado correctamente")
                else:
                    errores.append({'Archivo XML': nombre_logico, 'Estado': 'Sin líneas válidas'})
                    self.log("  Sin líneas válidas")
            except Exception as e:
                errores.append({'Archivo XML': nombre_logico, 'Estado': f'Error: {e}'})
                self.log(f"  Error: {e}")

        # ✅ anexar duplicados al reporte de errores (para que quede trazabilidad)
        if duplicados:
            errores.extend(duplicados)

        total_facturas = len(all_dfs)
        self.log(f"Total XML válidos para procesar (deduplicados): {total_facturas}")

        if not all_dfs:
            self.log("No se encontraron datos válidos.")
            if errores:
                try:
                    archivo_errores = self.carpeta_salida / f'errores_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
                    pl.DataFrame(errores).write_excel(str(archivo_errores))
                    self.log(f"Errores escritos en: {archivo_errores}")
                except Exception as e:
                    self.log(f"No se pudo escribir errores.xlsx: {e}")
            return

        # --------- Fase B: Consolidación y Excel ---------
        try:
            dfs_norm = [df for _, df in all_dfs]
            df_total = self._consolidar_para_excel(dfs_norm)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            archivo_salida = self.carpeta_salida / f'Facturas_Extraidas_{ts}.xlsx'
            archivo_errores = self.carpeta_salida / f'errores_{ts}.xlsx' if errores else None

            df_total_pd = df_total.to_pandas()
            df_cuatrimestral = self._generar_resumen_cuatrimestral(df_total_pd)
            # Hoja de compras (Asumiendo que agrupamos por el NIT del Emisor / Proveedor)
            df_proveedores_cuat = self._generar_resumen_proveedores(df_total_pd, is_clientes=False)
            # Hoja de ventas (Asumiendo que agrupamos por el NIT del Receptor / Cliente)
            df_clientes_cuat = self._generar_resumen_proveedores(df_total_pd, is_clientes=True)

            with pd.ExcelWriter(archivo_salida, engine="xlsxwriter") as writer:
                df_total_pd.to_excel(writer, sheet_name="Facturas", index=False)
                if not df_cuatrimestral.empty:
                    df_cuatrimestral.to_excel(writer, sheet_name="Declaración (Cuatrimestral)", index=False)
                if not df_proveedores_cuat.empty:
                    df_proveedores_cuat.to_excel(writer, sheet_name="Compras x Proveedor (Cuat.)", index=False)
                if not df_clientes_cuat.empty:
                    df_clientes_cuat.to_excel(writer, sheet_name="Ventas x Cliente (Cuat.)", index=False)

            if errores:
                pl.DataFrame(errores).write_excel(str(archivo_errores))
                self.log(f"{len(errores)} archivos con errores/duplicados. Ver: {archivo_errores}")

            self.log(f"✅ Excel generado: {archivo_salida}")
            self.log(f"Total de registros consolidados: {len(df_total_pd)}")

        except Exception as e:
            self.log(f"Error al generar el Excel: {e}")
            self._show_messagebox("error", "Error", f"Error al generar el Excel:\n{e}")
            return

        # --------- Fase C: Persistencia en BD ---------
        if not persist_db:
            self.log("Subida a BD deshabilitada (persist_db=False).")
            self._show_messagebox("info", "Proceso Completado", "Excel generado. No se realizó carga en BD.")
            return

        try:
            # 1️⃣ Crear batch y procesar las facturas en UNA SOLA CONEXIÓN
            batch_name = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            with get_session() as session:
                batch = save_batch(session, filename=batch_name, status="queued", tenant_id=self.tenant_id)
                session.flush()
                batch_id = batch.id
                self.log(f"Batch creado: {batch_name} (id={batch_id})")

                # 2️⃣ Procesar cada factura con commit anidado (savepoint)
                for idx, (file_name, df_doc) in enumerate(all_dfs, start=1):
                    if self._verificar_pausa_y_cancelacion():
                        self.log("🛑 Proceso cancelado por el usuario (Fase C - Base de datos).")
                        self._show_messagebox("warning", "Cancelado", "El guardado en BD fue cancelado por el usuario.")
                        return

                    nested = session.begin_nested()
                    try:
                        self.persistir_df_en_bd(session, batch_id, df_doc, self.tenant_id, file_name)
                        nested.commit()
                        self.log(f"Factura {idx}/{total_facturas} subida: {file_name} ✅")
                    except Exception as e:
                        nested.rollback()
                        errores.append({'Archivo XML': file_name, 'Estado': f'Error al insertar BD: {e}'})
                        self.log(f"❌ Error BD ({idx}/{total_facturas}): {file_name} - {str(e)}")

                # Hacemos commit final del batch y todas las facturas procesadas correctamente
                session.commit()

            # 3️⃣ Guardar errores BD si existen
            if errores:
                try:
                    archivo_errores_bd = self.carpeta_salida / f'errores_bd_{ts}.xlsx'
                    pl.DataFrame(errores).write_excel(str(archivo_errores_bd))
                    self.log(f"⚠ Errores BD escritos en: {archivo_errores_bd}")
                except Exception as e:
                    self.log(f"No se pudo escribir errores_bd.xlsx: {e}")

            self.log("✅ Todas las facturas procesadas correctamente.")
            self._show_messagebox("info", "Proceso Completado", f"Excel y carga en BD finalizados.\nTotal: {total_facturas}")

        except Exception as e:
            self.log(f"Error de conexión/subida en BD: {e}")
            self._show_messagebox("error", "Error BD", f"Error en base de datos:\n{e}")
