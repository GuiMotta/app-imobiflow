import urllib.parse
import streamlit as st
import pandas as pd
import plotly.express as px
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState, Disposition, Format

st.set_page_config(
    page_title="ImobiFlow - Dashboard Imóveis DF",
    page_icon="🏠",
    layout="wide"
)

st.markdown("""
<style>
[data-testid="metric-container"] {
    background: #f0f4ff;
    border-radius: 10px;
    padding: 10px;
    border-left: 4px solid #4C72B0;
}
.wa-btn a {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    background: #25D366;
    color: white !important;
    padding: 5px 14px;
    border-radius: 20px;
    font-size: 0.82rem;
    font-weight: 600;
    text-decoration: none;
}
.wa-btn a:hover { background: #1ebe5d; }
</style>
""", unsafe_allow_html=True)

# ── Helpers ───────────────────────────────────────────────────────────────────
def wa_link(texto: str) -> str:
    """Gera link wa.me com texto pré-formatado."""
    return f"https://wa.me/?text={urllib.parse.quote(texto)}"

def wa_button(texto: str, label: str = "📲 Compartilhar no WhatsApp"):
    """Renderiza botão verde do WhatsApp."""
    st.markdown(
        f'<div class="wa-btn"><a href="{wa_link(texto)}" target="_blank">{label}</a></div>',
        unsafe_allow_html=True
    )

# Config Plotly: desabilita zoom/pan nos gráficos (exceto mapa)
CHART_CONFIG = {"staticPlot": True}
MAP_CONFIG   = {"scrollZoom": True, "displayModeBar": False}

def chart_actions(fig, filename: str, wa_texto: str, wa_label: str = "📲 Compartilhar no WhatsApp"):
    """Renderiza [Baixar PNG] + [WhatsApp] lado a lado abaixo de cada gráfico."""
    try:
        img_bytes = fig.to_image(format="png", width=900, height=480, scale=2)
        col_dl, col_wa, _ = st.columns([1.4, 2, 4])
        with col_dl:
            st.download_button(
                label="📥 Baixar imagem",
                data=img_bytes,
                file_name=f"{filename}.png",
                mime="image/png",
                use_container_width=True,
            )
        with col_wa:
            wa_button(wa_texto, wa_label)
    except Exception:
        # Fallback caso kaleido não esteja disponível: só WhatsApp texto
        wa_button(wa_texto, wa_label)

# ── Databricks SDK ────────────────────────────────────────────────────────────
@st.cache_resource
def get_client():
    return WorkspaceClient()

@st.cache_resource
def get_warehouse_id():
    w = get_client()
    warehouses = list(w.warehouses.list())
    if not warehouses:
        return None
    for wh in warehouses:
        if wh.state and "RUNNING" in str(wh.state).upper():
            return wh.id
    return warehouses[0].id

# ── Carregamento de dados ─────────────────────────────────────────────────────
@st.cache_data(ttl=900)
def carregar_dados():
    w  = get_client()
    wh = get_warehouse_id()
    if not wh:
        st.error("❌ Nenhum SQL Warehouse disponível.")
        st.stop()

    sql = """
        SELECT
            bairro_vitrine                                      AS bairro,
            preco_atual                                         AS preco,
            area_util,
            quartos,
            banheiros,
            condominio,
            iptu,
            status,
            corretor_imobiliaria                                AS corretor,
            endereco_site                                       AS endereco,
            titulo_vitrine,
            descricao,
            url,
            CAST(coordenadas_oficiais.lat AS DOUBLE)            AS lat,
            CAST(coordenadas_oficiais.lon AS DOUBLE)            AS lon,
            data_cadastro_site                                  AS data_cadastro,
            -- preco_m2 apenas para áreas plausíveis (>= 15 m²) evita outliers de parse
            CASE
                WHEN area_util >= 15
                THEN ROUND(preco_atual / area_util, 0)
                ELSE NULL
            END                                                 AS preco_m2
        FROM gold.dfimoveis.`05_aln_imoveis_gold`
        WHERE preco_atual IS NOT NULL
        ORDER BY preco_atual
    """

    resp = w.statement_execution.execute_statement(
        warehouse_id=wh,
        statement=sql,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )

    if resp.status.state != StatementState.SUCCEEDED:
        st.error(f"❌ Query falhou: {resp.status}")
        st.stop()

    cols = [c.name for c in resp.manifest.schema.columns]
    rows = resp.result.data_array or []
    df   = pd.DataFrame(rows, columns=cols)

    for col in ["preco", "area_util", "quartos", "banheiros",
                "condominio", "iptu", "lat", "lon", "preco_m2"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Tipo de imóvel extraído da URL
    def detectar_tipo(url):
        u = str(url).lower()
        if any(k in u for k in ("apartamento", "/apto", "cobertura", "flat")):
            return "Apartamento"
        if any(k in u for k in ("casa-", "/sobrado", "townhouse", "geminado")):
            return "Casa"
        return "Outro"

    df["tipo_imovel"] = df["url"].apply(detectar_tipo)
    return df

df = carregar_dados()

if df.empty:
    st.warning("⚠️ Nenhum dado encontrado.")
    st.stop()

# ── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.image("https://img.icons8.com/color/96/home--v1.png", width=60)
st.sidebar.title("🔍 Filtros")

bairros_sel   = st.sidebar.multiselect("Bairro",  sorted(df["bairro"].dropna().unique()))
preco_min_v   = int(df["preco"].min())
preco_max_v   = int(df["preco"].max())
preco_range   = st.sidebar.slider("Preço (R$)", preco_min_v, preco_max_v,
                                   (preco_min_v, preco_max_v), step=10_000, format="R$ %d")
quartos_sel   = st.sidebar.multiselect("Quartos", sorted(df["quartos"].dropna().astype(int).unique()))
area_min_v    = int(df["area_util"].dropna().min())
area_max_v    = int(df["area_util"].dropna().max())
area_range    = st.sidebar.slider("Área (m²)", area_min_v, area_max_v,
                                   (area_min_v, area_max_v), step=5, format="%d m²")
status_sel    = st.sidebar.multiselect("Status",  sorted(df["status"].dropna().unique()))
corretor_opts = sorted(df["corretor"].dropna().unique())
corretor_sel  = st.sidebar.multiselect("Corretor / Imobiliária", corretor_opts)

# ── Filtros aplicados ─────────────────────────────────────────────────────────
dff = df.copy()
if bairros_sel:  dff = dff[dff["bairro"].isin(bairros_sel)]
if quartos_sel:  dff = dff[dff["quartos"].isin(quartos_sel)]
if status_sel:   dff = dff[dff["status"].isin(status_sel)]
if corretor_sel: dff = dff[dff["corretor"].isin(corretor_sel)]
dff = dff[(dff["preco"] >= preco_range[0]) & (dff["preco"] <= preco_range[1])]
# Área: inclui imóveis sem área cadastrada (NaN) + os que estão no range
dff = dff[dff["area_util"].isna() | dff["area_util"].between(area_range[0], area_range[1])]

filtro_label = ", ".join(bairros_sel) if bairros_sel else "Todos os bairros"

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🏠 ImobiFlow — Dashboard Imóveis DF")
st.caption("📦 Fonte: `gold.dfimoveis.05_aln_imoveis_gold`")
st.divider()

# ── KPIs ──────────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("📋 Total Imóveis",  f"{len(dff):,}")
k2.metric("💰 Preço Médio",    f"R$ {dff['preco'].mean():,.0f}")
k3.metric("💰 Preço Mediano",  f"R$ {dff['preco'].median():,.0f}")
k4.metric("📐 Área Média",     f"{dff['area_util'].mean():,.0f} m²")
k5.metric("🏷️ R$/m² Médio",    f"R$ {dff['preco_m2'].mean():,.0f}")

# WhatsApp — resumo do mercado
_txt_kpi = (
    f"🏠 *Mercado Imobiliário — {filtro_label}*\n\n"
    f"📋 Total de imóveis: {len(dff):,}\n"
    f"💰 Preço médio: R$ {dff['preco'].mean():,.0f}\n"
    f"💰 Preço mediano: R$ {dff['preco'].median():,.0f}\n"
    f"📐 Área média: {dff['area_util'].mean():,.0f} m²\n"
    f"🏷️ R$/m² médio: R$ {dff['preco_m2'].mean():,.0f}\n\n"
    f"📊 Dados atualizados via ImobiFlow Dashboard"
)
st.write("")
wa_button(_txt_kpi, "📲 Compartilhar resumo do mercado no WhatsApp")
st.divider()

# ── Linha 1 ───────────────────────────────────────────────────────────────────
col1, col2 = st.columns(2)
with col1:
    st.subheader("💵 Preço Médio por Bairro (Top 15)")
    avg = (dff.groupby("bairro")["preco"].mean()
              .sort_values(ascending=False).head(15).reset_index())
    avg["label"] = avg["preco"].apply(lambda x: f"R$ {x:,.0f}")
    fig = px.bar(avg, x="preco", y="bairro", orientation="h", text="label",
                 color="preco", color_continuous_scale="Blues",
                 labels={"preco": "Preço Médio (R$)", "bairro": "Bairro"})
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False, coloraxis_showscale=False,
                      yaxis=dict(autorange="reversed"), height=420)
    st.plotly_chart(fig, use_container_width=True, key="fig_preco_bairro", config=CHART_CONFIG)
    _top5 = avg.head(5)
    _txt = (
        f"💵 *Preço Médio por Bairro — Top 5*\n📍 {filtro_label}\n\n"
        + "\n".join(f"{i+1}. {r['bairro']}: R$ {r['preco']:,.0f}"
                    for i, r in _top5.iterrows())
        + "\n\n📊 ImobiFlow Dashboard"
    )
    chart_actions(fig, "preco_por_bairro", _txt)

with col2:
    st.subheader("📊 Distribuição de Preços")
    fig = px.histogram(dff, x="preco", nbins=30,
                       color_discrete_sequence=["#4C72B0"],
                       labels={"preco": "Preço (R$)", "count": "Imóveis"})
    fig.update_layout(height=420, showlegend=False, bargap=0.05)
    st.plotly_chart(fig, use_container_width=True, key="fig_dist_preco", config=CHART_CONFIG)
    _p25 = dff["preco"].quantile(0.25)
    _p75 = dff["preco"].quantile(0.75)
    _txt = (
        f"📊 *Distribuição de Preços — {filtro_label}*\n\n"
        f"💰 Mínimo: R$ {dff['preco'].min():,.0f}\n"
        f"💰 25%: R$ {_p25:,.0f}\n"
        f"💰 Mediana: R$ {dff['preco'].median():,.0f}\n"
        f"💰 75%: R$ {_p75:,.0f}\n"
        f"💰 Máximo: R$ {dff['preco'].max():,.0f}\n\n"
        f"📊 ImobiFlow Dashboard"
    )
    chart_actions(fig, "distribuicao_precos", _txt)

# ── Linha 2 ───────────────────────────────────────────────────────────────────
col3, col4 = st.columns(2)
with col3:
    st.subheader("📐 Área Útil × Preço")
    fig = px.scatter(dff.dropna(subset=["area_util", "preco"]),
                     x="area_util", y="preco", color="bairro",
                     hover_data=["endereco", "quartos", "preco_m2"],
                     labels={"area_util": "Área Útil (m²)", "preco": "Preço (R$)"},
                     opacity=0.7)
    fig.update_layout(height=380, showlegend=False)
    st.plotly_chart(fig, use_container_width=True, key="fig_area_preco", config=CHART_CONFIG)
    _txt = (
        f"📐 *Área Útil × Preço — {filtro_label}*\n\n"
        f"📐 Área mínima: {dff['area_util'].min():,.0f} m²\n"
        f"📐 Área média: {dff['area_util'].mean():,.0f} m²\n"
        f"📐 Área máxima: {dff['area_util'].max():,.0f} m²\n\n"
        f"📊 ImobiFlow Dashboard"
    )
    chart_actions(fig, "area_vs_preco", _txt)

with col4:
    st.subheader("🛏️ Imóveis por Nº de Quartos")
    qt = (dff["quartos"].dropna().astype(int)
              .value_counts().sort_index().reset_index())
    qt.columns = ["quartos", "quantidade"]
    fig = px.bar(qt, x="quartos", y="quantidade", text="quantidade",
                 color="quantidade", color_continuous_scale="Teal",
                 labels={"quartos": "Quartos", "quantidade": "Qtd."})
    fig.update_traces(textposition="outside")
    fig.update_layout(height=380, showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True, key="fig_quartos", config=CHART_CONFIG)
    _txt = (
        f"🛏️ *Imóveis por Nº de Quartos — {filtro_label}*\n\n"
        + "\n".join(f"  {int(r['quartos'])} quartos: {int(r['quantidade'])} imóveis"
                    for _, r in qt.iterrows())
        + "\n\n📊 ImobiFlow Dashboard"
    )
    chart_actions(fig, "imoveis_por_quartos", _txt)

# ── Linha 3 ───────────────────────────────────────────────────────────────────
col5, col6 = st.columns(2)
with col5:
    st.subheader("🏷️ R$/m² Médio por Bairro (Top 15)")
    avg_m2 = (dff.groupby("bairro")["preco_m2"].mean()
                 .sort_values(ascending=False).head(15).reset_index())
    avg_m2["label"] = avg_m2["preco_m2"].apply(lambda x: f"R$ {x:,.0f}")
    fig = px.bar(avg_m2, x="preco_m2", y="bairro", orientation="h", text="label",
                 color="preco_m2", color_continuous_scale="Oranges",
                 labels={"preco_m2": "R$/m²", "bairro": "Bairro"})
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False, coloraxis_showscale=False,
                      yaxis=dict(autorange="reversed"), height=420)
    st.plotly_chart(fig, use_container_width=True, key="fig_m2_bairro", config=CHART_CONFIG)
    _top5_m2 = avg_m2.head(5)
    _txt = (
        f"🏷️ *R$/m² Médio por Bairro — Top 5*\n📍 {filtro_label}\n\n"
        + "\n".join(f"{i+1}. {r['bairro']}: R$ {r['preco_m2']:,.0f}/m²"
                    for i, r in _top5_m2.iterrows())
        + "\n\n📊 ImobiFlow Dashboard"
    )
    chart_actions(fig, "rpm2_por_bairro", _txt)

with col6:
    st.subheader("📦 Box Plot de Preço por Quartos")
    top_q = dff["quartos"].dropna().astype(int).value_counts().head(5).index
    fig = px.box(dff[dff["quartos"].isin(top_q)].dropna(subset=["preco"]),
                 x="quartos", y="preco", color="quartos",
                 labels={"quartos": "Quartos", "preco": "Preço (R$)"},
                 category_orders={"quartos": sorted(top_q)})
    fig.update_layout(height=420, showlegend=False)
    st.plotly_chart(fig, use_container_width=True, key="fig_boxplot", config=CHART_CONFIG)
    _medians = (dff[dff["quartos"].isin(top_q)]
                .groupby("quartos")["preco"].median()
                .sort_index())
    _txt = (
        f"📦 *Preço Mediano por Nº de Quartos — {filtro_label}*\n\n"
        + "\n".join(f"  {int(q)} quartos: R$ {v:,.0f}"
                    for q, v in _medians.items())
        + "\n\n📊 ImobiFlow Dashboard"
    )
    chart_actions(fig, "boxplot_preco_quartos", _txt)

# ── Mapa ──────────────────────────────────────────────────────────────────────
df_map = dff.dropna(subset=["lat", "lon"]).copy()
df_map = df_map[(df_map["lat"].between(-16.5, -15.4)) &
                (df_map["lon"].between(-48.5, -47.2))]
df_map["area_util"] = df_map["area_util"].fillna(0).clip(lower=0)

if not df_map.empty:
    st.divider()
    st.subheader("🗺️ Mapa dos Imóveis")
    fig = px.scatter_mapbox(df_map, lat="lat", lon="lon",
                            color="preco", size="area_util", size_max=20,
                            hover_data=["bairro", "preco", "area_util",
                                        "quartos", "preco_m2", "endereco"],
                            color_continuous_scale="RdYlGn_r",
                            zoom=10.5, height=520, mapbox_style="carto-positron",
                            labels={"preco": "Preço (R$)"})
    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    st.plotly_chart(fig, use_container_width=True, key="fig_mapa", config=MAP_CONFIG)
    _txt = (
        f"🗺️ *Mapa de Imóveis — {filtro_label}*\n\n"
        f"📍 {len(df_map)} imóveis mapeados no Distrito Federal\n"
        f"💰 Preço médio: R$ {df_map['preco'].mean():,.0f}\n"
        f"🏷️ R$/m² médio: R$ {df_map['preco_m2'].mean():,.0f}\n\n"
        f"📊 ImobiFlow Dashboard"
    )
    chart_actions(fig, "mapa_imoveis", _txt, "📲 Compartilhar dados do mapa no WhatsApp")

# ── Tabela Detalhada ───────────────────────────────────────────────────────────
st.divider()
st.subheader("📋 Tabela Detalhada")

# Colunas exibidas (sem suites, vagas, status)
cols_tab = [c for c in ["bairro", "preco", "area_util", "preco_m2",
                         "quartos", "banheiros", "endereco", "corretor", "url"]
            if c in dff.columns]

df_tab = dff[cols_tab].copy()

# Pré-formata como string para evitar o '!' do NumberColumn
# (format com vírgula não é printf válido no Streamlit)
def fmt_moeda(v):  return f"R$ {v:,.0f}"  if pd.notna(v) and v > 0 else "—"
def fmt_area(v):   return f"{v:,.0f} m²"  if pd.notna(v) and v > 0 else "—"
def fmt_int(v):    return str(int(v))      if pd.notna(v) and v > 0 else "—"

df_tab["preco"]     = df_tab["preco"].apply(fmt_moeda)
df_tab["preco_m2"]  = df_tab["preco_m2"].apply(fmt_moeda)
df_tab["area_util"] = df_tab["area_util"].apply(fmt_area)
df_tab["quartos"]   = df_tab["quartos"].apply(fmt_int)
df_tab["banheiros"] = df_tab["banheiros"].apply(fmt_int)

df_tab = df_tab.rename(columns={
    "bairro":    "Bairro",
    "preco":     "Preço (R$)",
    "area_util": "Área (m²)",
    "preco_m2":  "R$/m²",
    "quartos":   "Quartos",
    "banheiros": "Banheiros",
    "endereco":  "Endereço",
    "corretor":  "Corretor/Imobiliária",
    "url":       "🔗 Ver no DFImóveis",
})

st.dataframe(
    df_tab,
    use_container_width=True,
    hide_index=True,
    height=420,
    column_config={
        "🔗 Ver no DFImóveis": st.column_config.LinkColumn(
                                   display_text="🏠 Abrir anúncio"
                               ),
    }
)

# WhatsApp — top 5 melhores ofertas (menor R$/m²)
_melhores = (dff.dropna(subset=["preco_m2", "url"])
               .sort_values("preco_m2")
               .head(5))
if not _melhores.empty:
    _linhas = "\n".join(
        f"{i+1}. {r['bairro']} | {int(r['quartos']) if pd.notna(r['quartos']) else '?'}q "
        f"| R$ {r['preco']:,.0f} | R$/m² {r['preco_m2']:,.0f}\n   🔗 {r['url']}"
        for i, (_, r) in enumerate(_melhores.iterrows())
    )
    _txt_tab = (
        f"🏆 *Melhores Ofertas por R$/m² — {filtro_label}*\n\n"
        f"{_linhas}\n\n"
        f"📊 ImobiFlow Dashboard"
    )
    wa_button(_txt_tab, "📲 Compartilhar melhores ofertas no WhatsApp")

# ── Oportunidade de Captação ───────────────────────────────────────────────────
import re

# Regex para detectar Bloco (apartamentos) e Conjunto (casas) nos campos de texto
_RE_BLOCO    = re.compile(r'\b(bl\.?|bloco)\s*[a-z0-9]+', re.IGNORECASE)
_RE_CONJUNTO = re.compile(r'\b(conj\.?|conjunto)\s*[0-9a-z]+', re.IGNORECASE)

def _match_campos(row, regex):
    """Verifica se o regex bate em qualquer um dos campos de texto relevantes."""
    for campo in ("titulo_vitrine", "descricao", "endereco"):
        val = row.get(campo) or ""
        if regex.search(str(val)):
            return True
    return False

def _extrair_referencia(row, regex):
    """Extrai o trecho que deu match (bloco / conjunto) para exibir na grid."""
    for campo in ("endereco", "titulo_vitrine", "descricao"):
        val = row.get(campo) or ""
        m = regex.search(str(val))
        if m:
            # Retorna até 30 chars a partir do match para dar contexto
            start = m.start()
            return str(val)[max(0, start - 5):start + 30].strip()
    return ""

# Aplica sobre o df filtrado (respeita filtros da sidebar)
df_capt = dff.copy()

mask_apto = (df_capt["tipo_imovel"] == "Apartamento") & \
            df_capt.apply(lambda r: _match_campos(r, _RE_BLOCO), axis=1)
mask_casa = (df_capt["tipo_imovel"] == "Casa") & \
            df_capt.apply(lambda r: _match_campos(r, _RE_CONJUNTO), axis=1)

df_apto_capt = df_capt[mask_apto].copy()
df_casa_capt = df_capt[mask_casa].copy()

df_apto_capt["Bloco"] = df_apto_capt.apply(
    lambda r: _extrair_referencia(r, _RE_BLOCO), axis=1)
df_casa_capt["Conjunto"] = df_casa_capt.apply(
    lambda r: _extrair_referencia(r, _RE_CONJUNTO), axis=1)

st.divider()
st.subheader("🎯 Oportunidade de Captação")
st.caption(
    "Imóveis com **endereço completo** identificado (Bloco para aptos / Conjunto para casas) — "
    "ideais para abordagem direta de captação."
)

_tab_apto, _tab_casa = st.tabs([
    f"🏢 Apartamentos ({len(df_apto_capt)})",
    f"🏠 Casas ({len(df_casa_capt)})",
])

# Colunas base para as grids
_COLS_CAPT = ["bairro", "preco", "area_util", "preco_m2",
              "quartos", "banheiros", "endereco", "corretor", "url"]

def _montar_grid_capt(df_raw, col_extra: str):
    """Monta e exibe a grid de captação com a coluna de referência (Bloco ou Conjunto)."""
    if df_raw.empty:
        st.info("Nenhum imóvel encontrado com os filtros atuais.")
        return

    cols = [c for c in _COLS_CAPT if c in df_raw.columns]
    df_g = df_raw[cols + [col_extra]].copy()

    # Formata valores
    df_g["preco"]     = df_g["preco"].apply(fmt_moeda)
    df_g["preco_m2"]  = df_g["preco_m2"].apply(fmt_moeda)
    df_g["area_util"] = df_g["area_util"].apply(fmt_area)
    df_g["quartos"]   = df_g["quartos"].apply(fmt_int)
    df_g["banheiros"] = df_g["banheiros"].apply(fmt_int)

    df_g = df_g.rename(columns={
        "bairro":    "Bairro",
        "preco":     "Preço (R$)",
        "area_util": "Área (m²)",
        "preco_m2":  "R$/m²",
        "quartos":   "Quartos",
        "banheiros": "Banheiros",
        "endereco":  "Endereço",
        "corretor":  "Corretor/Imobiliária",
        "url":       "🔗 Ver anúncio",
    })

    # Move a coluna de referência para o início
    cols_ordem = [col_extra, "Bairro", "Endereço", "Preço (R$)", "R$/m²",
                  "Área (m²)", "Quartos", "Banheiros", "Corretor/Imobiliária", "🔗 Ver anúncio"]
    cols_ordem = [c for c in cols_ordem if c in df_g.columns]
    df_g = df_g[cols_ordem]

    st.dataframe(
        df_g,
        use_container_width=True,
        hide_index=True,
        height=420,
        column_config={
            "🔗 Ver anúncio": st.column_config.LinkColumn(display_text="🏠 Abrir"),
        },
    )

    # Botão WhatsApp com top 5
    _top5 = df_raw.dropna(subset=["preco_m2"]).sort_values("preco_m2").head(5)
    if not _top5.empty:
        _ref_col = col_extra
        _linhas = "\n".join(
            f"{i+1}. {r['bairro']} | {r.get(_ref_col, '')} | "
            f"R$ {r['preco']:,.0f} | {r['url']}"
            for i, (_, r) in enumerate(_top5.iterrows())
        )
        _tipo_label = "Apartamentos" if col_extra == "Bloco" else "Casas"
        _txt_wa = (
            f"🎯 *Oportunidades de Captação — {_tipo_label}*\n"
            f"📍 {filtro_label}\n\n{_linhas}\n\n📊 ImobiFlow Dashboard"
        )
        wa_button(_txt_wa, f"📲 Compartilhar oportunidades ({_tipo_label}) no WhatsApp")

with _tab_apto:
    _montar_grid_capt(df_apto_capt, "Bloco")

with _tab_casa:
    _montar_grid_capt(df_casa_capt, "Conjunto")

# ── Oportunidade de Venda ──────────────────────────────────────────────────────
st.divider()
st.subheader("💡 Oportunidade de Venda")
st.caption(
    "Imóveis com **R$/m² abaixo ou próximo da média do bairro** — "
    "potencial de valorização ou venda rápida por preço competitivo."
)

# Controle do threshold pelo corretor
_col_thr, _ = st.columns([2, 5])
with _col_thr:
    _threshold = st.slider(
        "Máximo acima da média do bairro (%)",
        min_value=-50, max_value=20, value=0, step=5,
        format="%d%%",
        help="0% = apenas abaixo da média | 10% = inclui até 10% acima da média"
    )

# Calcula média de R$/m² por bairro (usa o df completo, sem filtros, como referência de mercado)
_media_bairro = (
    df.dropna(subset=["preco_m2", "bairro"])
    .groupby("bairro")["preco_m2"]
    .mean()
    .rename("media_m2_bairro")
)

# Aplica sobre o df filtrado + apenas imóveis com preco_m2 válido
df_venda = dff.dropna(subset=["preco_m2"]).copy()
df_venda = df_venda.join(_media_bairro, on="bairro")

df_venda["var_vs_media_pct"] = (
    (df_venda["preco_m2"] - df_venda["media_m2_bairro"])
    / df_venda["media_m2_bairro"] * 100
).round(1)

# Filtra pelo threshold escolhido
df_venda = df_venda[df_venda["var_vs_media_pct"] <= _threshold].sort_values("var_vs_media_pct")

# KPIs rápidos
_kv1, _kv2, _kv3, _ = st.columns([1, 1, 1, 3])
_kv1.metric("📋 Oportunidades", f"{len(df_venda):,}")
if not df_venda.empty:
    _kv2.metric("🏷️ Maior desconto",
                f"{df_venda['var_vs_media_pct'].min():+.1f}%")
    _kv3.metric("💰 Menor R$/m²",
                f"R$ {df_venda['preco_m2'].min():,.0f}")

st.write("")

if df_venda.empty:
    st.info("Nenhum imóvel encontrado com os critérios atuais.")
else:
    # Monta grid
    _cols_v = ["bairro", "preco", "area_util", "preco_m2",
               "media_m2_bairro", "var_vs_media_pct",
               "quartos", "banheiros", "endereco", "corretor", "url"]
    _cols_v = [c for c in _cols_v if c in df_venda.columns]
    df_gv = df_venda[_cols_v].copy()

    # Formata
    df_gv["preco"]          = df_gv["preco"].apply(fmt_moeda)
    df_gv["area_util"]      = df_gv["area_util"].apply(fmt_area)
    df_gv["preco_m2"]       = df_gv["preco_m2"].apply(fmt_moeda)
    df_gv["media_m2_bairro"]= df_gv["media_m2_bairro"].apply(fmt_moeda)
    df_gv["var_vs_media_pct"]= df_gv["var_vs_media_pct"].apply(
        lambda v: f"{v:+.1f}%" if pd.notna(v) else "—"
    )
    df_gv["quartos"]        = df_gv["quartos"].apply(fmt_int)
    df_gv["banheiros"]      = df_gv["banheiros"].apply(fmt_int)

    df_gv = df_gv.rename(columns={
        "bairro":           "Bairro",
        "preco":            "Preço (R$)",
        "area_util":        "Área (m²)",
        "preco_m2":         "R$/m²",
        "media_m2_bairro":  "Média R$/m² Bairro",
        "var_vs_media_pct": "vs. Média",
        "quartos":          "Quartos",
        "banheiros":        "Banheiros",
        "endereco":         "Endereço",
        "corretor":         "Corretor/Imobiliária",
        "url":              "🔗 Ver anúncio",
    })

    st.dataframe(
        df_gv,
        use_container_width=True,
        hide_index=True,
        height=480,
        column_config={
            "🔗 Ver anúncio": st.column_config.LinkColumn(display_text="🏠 Abrir"),
        },
    )

    # WhatsApp — top 5 mais abaixo da média
    _top5v = df_venda.head(5)
    _linhas_v = "\n".join(
        f"{i+1}. {r['bairro']} | R$/m² {r['preco_m2']:,.0f} "
        f"({r['var_vs_media_pct']:+.1f}% vs média) | R$ {r['preco']:,.0f}\n   🔗 {r['url']}"
        for i, (_, r) in enumerate(_top5v.iterrows())
    )
    _txt_venda = (
        f"💡 *Oportunidades de Venda — {filtro_label}*\n"
        f"_(imóveis abaixo ou na média de R$/m² do bairro)_\n\n"
        f"{_linhas_v}\n\n📊 ImobiFlow Dashboard"
    )
    wa_button(_txt_venda, "📲 Compartilhar oportunidades de venda no WhatsApp")

st.divider()
st.caption("🚀 ImobiFlow © 2026 | Delta Lake | `gold.dfimoveis.05_aln_imoveis_gold`")
