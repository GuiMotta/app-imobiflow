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

st.divider()
st.caption("🚀 ImobiFlow © 2026 | Delta Lake | `gold.dfimoveis.05_aln_imoveis_gold`")
