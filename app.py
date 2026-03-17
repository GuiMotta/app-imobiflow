import streamlit as st
import pandas as pd
import plotly.express as px
from pyspark.sql import SparkSession

st.set_page_config(
    page_title="ImobiFlow - Dashboard Imóveis DF",
    page_icon="🏠",
    layout="wide"
)

# ── CSS ──────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
[data-testid="metric-container"] {
    background: #f0f4ff;
    border-radius: 10px;
    padding: 10px;
    border-left: 4px solid #4C72B0;
}
.stDataFrame { border-radius: 10px; }
</style>
""", unsafe_allow_html=True)

# ── Spark ────────────────────────────────────────────────────────────────────
spark = SparkSession.builder.getOrCreate()

# ── Carregamento de dados ─────────────────────────────────────────────────────
@st.cache_data(ttl=900)
def carregar_dados():
    query = """
        SELECT
            bairro_vitrine                                  AS bairro,
            preco_atual                                     AS preco,
            area_util,
            quartos,
            suites,
            vagas,
            banheiros,
            condominio,
            iptu,
            status,
            corretor_imobiliaria                            AS corretor,
            endereco_site                                   AS endereco,
            CAST(coordenadas_oficiais.lat AS DOUBLE)        AS lat,
            CAST(coordenadas_oficiais.lon AS DOUBLE)        AS lon,
            data_cadastro_site                              AS data_cadastro,
            ROUND(preco_atual / NULLIF(area_util, 0), 0)   AS preco_m2
        FROM gold.dfimoveis.`05_aln_imoveis_gold`
        WHERE preco_atual IS NOT NULL
        ORDER BY preco_atual
    """
    df = spark.sql(query).toPandas()
    for col in ["preco", "area_util", "quartos", "suites", "vagas",
                "banheiros", "condominio", "iptu", "lat", "lon", "preco_m2"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

df = carregar_dados()

if df.empty:
    st.warning("⚠️ Nenhum dado encontrado na tabela gold.")
    st.stop()

# ── Sidebar – Filtros ─────────────────────────────────────────────────────────
st.sidebar.image("https://img.icons8.com/color/96/home--v1.png", width=60)
st.sidebar.title("🔍 Filtros")

bairros_disp = sorted(df["bairro"].dropna().unique())
bairros_sel  = st.sidebar.multiselect("Bairro", bairros_disp)

preco_min_v = int(df["preco"].min())
preco_max_v = int(df["preco"].max())
preco_range = st.sidebar.slider(
    "Preço (R$)", preco_min_v, preco_max_v,
    (preco_min_v, preco_max_v),
    step=10_000, format="R$ %d"
)

quartos_opts = sorted(df["quartos"].dropna().astype(int).unique())
quartos_sel  = st.sidebar.multiselect("Quartos", quartos_opts)

status_opts = sorted(df["status"].dropna().unique())
status_sel  = st.sidebar.multiselect("Status", status_opts)

# ── Aplicar filtros ───────────────────────────────────────────────────────────
dff = df.copy()
if bairros_sel:
    dff = dff[dff["bairro"].isin(bairros_sel)]
if quartos_sel:
    dff = dff[dff["quartos"].isin(quartos_sel)]
if status_sel:
    dff = dff[dff["status"].isin(status_sel)]
dff = dff[(dff["preco"] >= preco_range[0]) & (dff["preco"] <= preco_range[1])]

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🏠 ImobiFlow — Dashboard Imóveis DF")
st.caption("📦 Fonte: `gold.dfimoveis.05_aln_imoveis_gold`")
st.divider()

# ── KPIs ──────────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("📋 Total Imóveis",   f"{len(dff):,}")
k2.metric("💰 Preço Médio",     f"R$ {dff['preco'].mean():,.0f}")
k3.metric("💰 Preço Mediano",   f"R$ {dff['preco'].median():,.0f}")
k4.metric("📐 Área Média",      f"{dff['area_util'].mean():,.0f} m²")
k5.metric("🏷️ R$/m² Médio",     f"R$ {dff['preco_m2'].mean():,.0f}")

st.divider()

# ── Linha 1: Preço por Bairro + Histograma ────────────────────────────────────
col1, col2 = st.columns(2)

with col1:
    st.subheader("💵 Preço Médio por Bairro (Top 15)")
    avg_bairro = (
        dff.groupby("bairro")["preco"]
           .mean()
           .sort_values(ascending=False)
           .head(15)
           .reset_index()
    )
    avg_bairro["label"] = avg_bairro["preco"].apply(lambda x: f"R$ {x:,.0f}")
    fig1 = px.bar(
        avg_bairro, x="preco", y="bairro", orientation="h",
        text="label", color="preco",
        color_continuous_scale="Blues",
        labels={"preco": "Preço Médio (R$)", "bairro": "Bairro"}
    )
    fig1.update_traces(textposition="outside")
    fig1.update_layout(
        showlegend=False, coloraxis_showscale=False,
        yaxis=dict(autorange="reversed"), height=420
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("📊 Distribuição de Preços")
    fig2 = px.histogram(
        dff, x="preco", nbins=30,
        color_discrete_sequence=["#4C72B0"],
        labels={"preco": "Preço (R$)", "count": "Qtd. Imóveis"}
    )
    fig2.update_layout(height=420, showlegend=False,
                       bargap=0.05)
    st.plotly_chart(fig2, use_container_width=True)

# ── Linha 2: Scatter Área x Preço + Quartos ──────────────────────────────────
col3, col4 = st.columns(2)

with col3:
    st.subheader("📐 Área Útil × Preço")
    fig3 = px.scatter(
        dff.dropna(subset=["area_util", "preco"]),
        x="area_util", y="preco",
        color="bairro",
        hover_data=["endereco", "quartos", "preco_m2"],
        labels={"area_util": "Área Útil (m²)", "preco": "Preço (R$)"},
        opacity=0.7
    )
    fig3.update_layout(height=380, showlegend=False)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader("🛏️ Imóveis por Nº de Quartos")
    qt = (
        dff["quartos"].dropna().astype(int)
           .value_counts().sort_index().reset_index()
    )
    qt.columns = ["quartos", "quantidade"]
    fig4 = px.bar(
        qt, x="quartos", y="quantidade",
        text="quantidade", color="quantidade",
        color_continuous_scale="Teal",
        labels={"quartos": "Quartos", "quantidade": "Qtd. Imóveis"}
    )
    fig4.update_traces(textposition="outside")
    fig4.update_layout(height=380, showlegend=False,
                       coloraxis_showscale=False)
    st.plotly_chart(fig4, use_container_width=True)

# ── Linha 3: Preço/m² por Bairro + Box Plot ──────────────────────────────────
col5, col6 = st.columns(2)

with col5:
    st.subheader("🏷️ R$/m² Médio por Bairro (Top 15)")
    avg_m2 = (
        dff.groupby("bairro")["preco_m2"]
           .mean()
           .sort_values(ascending=False)
           .head(15)
           .reset_index()
    )
    avg_m2["label"] = avg_m2["preco_m2"].apply(lambda x: f"R$ {x:,.0f}")
    fig5 = px.bar(
        avg_m2, x="preco_m2", y="bairro", orientation="h",
        text="label", color="preco_m2",
        color_continuous_scale="Oranges",
        labels={"preco_m2": "R$/m²", "bairro": "Bairro"}
    )
    fig5.update_traces(textposition="outside")
    fig5.update_layout(
        showlegend=False, coloraxis_showscale=False,
        yaxis=dict(autorange="reversed"), height=420
    )
    st.plotly_chart(fig5, use_container_width=True)

with col6:
    st.subheader("📦 Distribuição de Preços por Nº Quartos")
    top_bairros = dff["quartos"].dropna().astype(int).value_counts().head(5).index
    fig6 = px.box(
        dff[dff["quartos"].isin(top_bairros)].dropna(subset=["preco"]),
        x="quartos", y="preco",
        color="quartos",
        labels={"quartos": "Quartos", "preco": "Preço (R$)"},
        category_orders={"quartos": sorted(top_bairros)}
    )
    fig6.update_layout(height=420, showlegend=False)
    st.plotly_chart(fig6, use_container_width=True)

# ── Mapa ──────────────────────────────────────────────────────────────────────
df_map = dff.dropna(subset=["lat", "lon"])
df_map = df_map[
    (df_map["lat"].between(-16.5, -15.4)) &
    (df_map["lon"].between(-48.5, -47.2))
]

if not df_map.empty:
    st.divider()
    st.subheader("🗺️ Mapa dos Imóveis")
    fig_map = px.scatter_mapbox(
        df_map,
        lat="lat", lon="lon",
        color="preco",
        size="area_util",
        size_max=20,
        hover_data=["bairro", "preco", "area_util", "quartos",
                    "preco_m2", "endereco"],
        color_continuous_scale="RdYlGn_r",
        zoom=10.5,
        height=520,
        mapbox_style="carto-positron",
        labels={"preco": "Preço (R$)"}
    )
    fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    st.plotly_chart(fig_map, use_container_width=True)

# ── Tabela Detalhada ──────────────────────────────────────────────────────────
st.divider()
st.subheader("📋 Tabela Detalhada")

cols_show = ["bairro", "preco", "area_util", "preco_m2",
             "quartos", "suites", "vagas", "status",
             "endereco", "corretor"]
cols_show = [c for c in cols_show if c in dff.columns]

st.dataframe(
    dff[cols_show].rename(columns={
        "bairro":    "Bairro",
        "preco":     "Preço (R$)",
        "area_util": "Área (m²)",
        "preco_m2":  "R$/m²",
        "quartos":   "Quartos",
        "suites":    "Suítes",
        "vagas":     "Vagas",
        "status":    "Status",
        "endereco":  "Endereço",
        "corretor":  "Corretor/Imobiliária"
    }).style.format({
        "Preço (R$)": "R$ {:,.0f}",
        "R$/m²":      "R$ {:,.0f}",
        "Área (m²)":  "{:,.0f} m²",
    }),
    use_container_width=True,
    hide_index=True,
    height=420
)

# ── Rodapé ────────────────────────────────────────────────────────────────────
st.divider()
st.caption("🚀 ImobiFlow © 2026 | Pipeline Delta Lake | `gold.dfimoveis.05_aln_imoveis_gold`")
