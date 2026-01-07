import os
import requests
import pandas as pd
from datetime import date, timedelta

# =============== CONFIGURAÇÕES ===============
URL = "https://www.anbima.com.br/informacoes/curvas-debentures/CD-down.asp"

EXCEL_DESTINO = "BaseMercado.xlsx"
ABA_DESTINO = "Curvas_ANBIMA"

JANELA_DIAS = 7  # site só libera 7 dias

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.anbima.com.br/informacoes/curvas-debentures/",
}
# ===========================================


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Transforma colunas MultiIndex em strings simples."""
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join(
                [str(x).strip() for x in tup if str(x).strip() and str(x).strip().lower() != "nan"]
            ).strip()
            for tup in df.columns.to_list()
        ]
    else:
        df.columns = [str(c).strip() for c in df.columns]
    return df


def _corrigir_decimais_ptbr(df: pd.DataFrame) -> pd.DataFrame:
    """Converte números pt-BR (1.234,56) para float."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == "object":
            s = df[col].astype(str)
            if s.str.contains(r"\d", regex=True).any() and (
                s.str.contains(",", regex=False).any() or s.str.contains(".", regex=False).any()
            ):
                s2 = (
                    s.str.replace("\u00a0", "", regex=False)
                     .str.replace(" ", "", regex=False)
                     .str.replace(".", "", regex=False)
                     .str.replace(",", ".", regex=False)
                )
                df[col] = pd.to_numeric(s2, errors="ignore")
    return df


def baixar_curva(data_ref: date) -> pd.DataFrame | None:
    """Baixa a curva para data_ref. Retorna DF ou None."""
    dt_ref_str = data_ref.strftime("%d/%m/%Y")
    payload = {"Idioma": "PT", "Dt_Ref": dt_ref_str, "saida": "xls"}

    resp = requests.post(URL, headers=headers, data=payload, timeout=30)
    if resp.status_code != 200 or not resp.text.strip():
        return None

    try:
        tabelas = pd.read_html(resp.text, decimal=",", thousands=".")
        df = tabelas[0].copy()

        df = _flatten_columns(df)

        # 🔹 Data como primeira coluna (texto dd/mm/aaaa)
        df["Data"] = data_ref.strftime("%d/%m/%Y")
        df.insert(0, "Data", df.pop("Data"))

        df = _corrigir_decimais_ptbr(df)

        return df
    except Exception as e:
        print(f"Falhou para {dt_ref_str}: {e}")
        return None


# ========= 1) Ler base existente =========
df_existente = None
datas_existentes = set()

if os.path.exists(EXCEL_DESTINO):
    try:
        df_existente = pd.read_excel(EXCEL_DESTINO, sheet_name=ABA_DESTINO)
        df_existente = _flatten_columns(df_existente)

        if "Data" in df_existente.columns:
            datas_existentes = set(
                pd.to_datetime(
                    df_existente["Data"], format="%d/%m/%Y", errors="coerce"
                ).dt.date.dropna().unique()
            )
    except ValueError:
        df_existente = None
        datas_existentes = set()

print(f"Datas já existentes: {len(datas_existentes)}")


# ========= 2) Baixar últimos 7 dias =========
hoje = date.today()
novos = []
pulados = 0
baixados = 0

for i in range(JANELA_DIAS):
    d = hoje - timedelta(days=i)

    if d in datas_existentes:
        pulados += 1
        continue

    df = baixar_curva(d)
    if df is None:
        print(f"Sem curva para {d} (ok).")
        continue

    novos.append(df)
    baixados += 1
    print(f"✅ Baixou {d}")

print(f"\nResumo: baixados={baixados}, pulados_por_repetição={pulados}")

if not novos:
    print("Nada novo para salvar.")
    raise SystemExit(0)

df_novos = pd.concat(novos, ignore_index=True)
df_novos = _flatten_columns(df_novos)


# ========= 3) Concatenar =========
if df_existente is not None:
    df_final = pd.concat([df_existente, df_novos], ignore_index=True)
else:
    df_final = df_novos

df_final = _flatten_columns(df_final)

# ========= 4) ORDENAR (DATAS ANTIGAS EM CIMA) =========
if "Data" in df_final.columns:
    df_final["_Data_ord"] = pd.to_datetime(
        df_final["Data"], format="%d/%m/%Y", errors="coerce"
    )
    df_final = df_final.sort_values("_Data_ord", ascending=True)
    df_final = df_final.drop(columns="_Data_ord")
    df_final.insert(0, "Data", df_final.pop("Data"))
    df_final = df_final.reset_index(drop=True)


# ========= 5) SALVAR =========
mode = "a" if os.path.exists(EXCEL_DESTINO) else "w"
writer_kwargs = {"if_sheet_exists": "replace"} if mode == "a" else {}

with pd.ExcelWriter(EXCEL_DESTINO, engine="openpyxl", mode=mode, **writer_kwargs) as writer:
    df_final.to_excel(writer, sheet_name=ABA_DESTINO, index=False)

print(f"\n💾 Salvo em {EXCEL_DESTINO} (aba {ABA_DESTINO}). Total de linhas: {len(df_final)}")
# export
