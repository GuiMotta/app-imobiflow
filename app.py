import urllib.parse
import datetime
import re
import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2

st.set_page_config(
    page_title="ImobiFlow - Dashboard Imóveis DF",
    page_icon="🏠",
    layout="wide"
)

# ── Autenticação simples ────────────────────────────────────────────────────
def check_password():
    if st.session_state.get("authenticated"):
        return True
    with st.container():
        st.markdown("## 🔐 ImobiFlow — Acesso Restrito")
        pwd = st.text_input("Senha de acesso", type="password", key="pwd_input")
        if st.button("Entrar", use_container_width=False):
            if pwd == st.secrets["APP_PASSWORD"]:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Senha incorreta.")
    return False

if not check_password():
    st.stop()

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
    return f"https://wa.me/?text={urllib.parse.quote(texto)}"

def wa_button(texto: str, label: str = "📲 Compartilhar no WhatsApp"):
    st.markdown(
        f'<div class="wa-btn"><a href="{wa_link(texto)}" target="_blank">{label}</a></div>',
        unsafe_allow_html=True
    )

def _br(v):
    """Formata número inteiro no padrão brasileiro: 1.099.758"""
    return f"{v:,.0f}".replace(",", ".")

def fmt_moeda(v):  return f"R$ {_br(v)}"   if pd.notna(v) and v > 0 else "—"
def fmt_area(v):   return f"{_br(v)} m²"   if pd.notna(v) and v > 0 else "—"
def fmt_int(v):    return str(int(v))       if pd.notna(v) and v > 0 else "—"

def wa_imovel_link(url):
    """Gera link wa.me com a URL do imóvel pré-preenchida."""
    if pd.notna(url) and str(url).startswith("http"):
        return wa_link(f"Confira este imóvel: {url}")
    return ""

CHART_CONFIG = {"displayModeBar": "hover"}   # barra aparece ao passar o mouse
MAP_CONFIG   = {"scrollZoom": True, "displayModeBar": False}  # mapa sem barra (ocupa espaço)

def chart_actions(fig, filename: str, wa_texto: str, wa_label: str = "📲 Compartilhar no WhatsApp"):
    try:
        img_bytes = fig.to_image(format="png", width=900, height=480, scale=2)
        col_dl, col_wa, _ = st.columns([1.4, 2, 4])
        with col_dl:
            st.download_button(label="📥 Baixar imagem", data=img_bytes,
                               file_name=f"{filename}.png", mime="image/png",
                               use_container_width=True)
        with col_wa:
            wa_button(wa_texto, wa_label)
    except Exception:
        wa_button(wa_texto, wa_label)

def montar_grid(df_raw, key_prefix: str, cols_base=None, col_extra=None, altura=480):
    """
    Monta grid com colunas formatadas + coluna WhatsApp por imóvel.
    col_extra: nome de coluna extra (ex: 'Bloco', 'Conjunto') a inserir no início.
    """
    if cols_base is None:
        cols_base = ["bairro", "preco", "area_util", "preco_m2",
                     "quartos", "banheiros", "endereco", "corretor", "url"]

    cols = [c for c in cols_base if c in df_raw.columns]
    df_g = df_raw[cols + ([col_extra] if col_extra and col_extra in df_raw.columns else [])].copy()

    # Coluna WhatsApp por imóvel (antes de renomear)
    df_g["📲 WA"] = df_g["url"].apply(wa_imovel_link) if "url" in df_g.columns else ""

    # Formatações numéricas
    for c, fn in [("preco", fmt_moeda), ("preco_m2", fmt_moeda),
                  ("area_util", fmt_area), ("quartos", fmt_int), ("banheiros", fmt_int)]:
        if c in df_g.columns:
            df_g[c] = df_g[c].apply(fn)
    if "media_m2_bairro" in df_g.columns:
        df_g["media_m2_bairro"] = df_g["media_m2_bairro"].apply(fmt_moeda)
    if "var_vs_media_pct" in df_g.columns:
        df_g["var_vs_media_pct"] = df_g["var_vs_media_pct"].apply(
            lambda v: f"{v:+.1f}%" if pd.notna(v) else "—"
        )

    df_g = df_g.rename(columns={
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

    # Ordem das colunas
    prioridade = ([col_extra] if col_extra else []) + [
        "Bairro", "Endereço", "Preço (R$)", "R$/m²", "Média R$/m² Bairro", "vs. Média",
        "Área (m²)", "Quartos", "Banheiros", "Corretor/Imobiliária", "🔗 Ver anúncio", "📲 WA"
    ]
    df_g = df_g[[c for c in prioridade if c in df_g.columns]]

    st.dataframe(
        df_g,
        use_container_width=True,
        hide_index=True,
        height=altura,
        column_config={
            "🔗 Ver anúncio": st.column_config.LinkColumn(display_text="🏠 Abrir"),
            "📲 WA":          st.column_config.LinkColumn(display_text="📲 WhatsApp"),
        },
    )

# ── Supabase PostgreSQL ───────────────────────────────────────────────────────
@st.cache_resource
def get_connection_string():
    return st.secrets["DATABASE_URL"]

# ── Carregamento de dados ─────────────────────────────────────────────────────
@st.cache_data(ttl=900)
def carregar_dados():
    conn_str = get_connection_string()

    sql = """
        SELECT
            bairro,
            preco,
            area_util,
            quartos,
            banheiros,
            condominio,
            iptu,
            status,
            corretor,
            endereco,
            titulo_vitrine,
            LEFT(descricao, 300) AS descricao,
            url,
            lat,
            lon,
            data_cadastro,
            dt_inativo,
            preco_m2
        FROM public.imoveis
        WHERE preco IS NOT NULL
        ORDER BY preco
    """

    try:
        conn = psycopg2.connect(conn_str, sslmode="require")
        df = pd.read_sql(sql, conn)
        conn.close()
    except Exception as e:
        st.error(f"❌ Erro ao conectar ao banco: {e}")
        st.stop()

    for c in ["preco", "area_util", "quartos", "banheiros",
              "condominio", "iptu", "lat", "lon", "preco_m2"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Remover outliers absurdos (erros de cadastro no site)
    df.loc[df["preco"] > 50_000_000, "preco"] = None
    df.loc[df["area_util"] > 10_000, "area_util"] = None
    df["preco_m2"] = df.apply(
        lambda r: r["preco"] / r["area_util"]
        if pd.notna(r["preco"]) and pd.notna(r["area_util"]) and r["area_util"] > 0
        else None, axis=1
    )
    df = df[df["preco"].notna()]

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

# 1. Localização
bairros_sel   = st.sidebar.multiselect("📍 Bairro", sorted(df["bairro"].dropna().unique()))

# 2. Características do imóvel
quartos_sel   = st.sidebar.multiselect("🛏️ Quartos", sorted(df["quartos"].dropna().astype(int).unique()))

preco_min_v   = int(df["preco"].min())
preco_max_v   = int(df["preco"].max())
preco_range   = st.sidebar.slider("💰 Preço (R$)", preco_min_v, preco_max_v,
                                   (preco_min_v, preco_max_v), step=10_000, format="R$ %d")

area_min_v    = int(df["area_util"].dropna().min())
area_max_v    = int(df["area_util"].dropna().max())
area_range    = st.sidebar.slider("📐 Área (m²)", area_min_v, area_max_v,
                                   (area_min_v, area_max_v), step=5, format="%d m²")

# 3. Recência
st.sidebar.markdown("**📅 Cadastrado no site**")
_PERIODOS = {
    "Todos": None, "Hoje": 0, "Últimos 7 dias": 7,
    "Últimos 15 dias": 15, "Últimos 30 dias": 30,
    "Últimos 90 dias": 90, "Personalizado": -1,
}
_periodo_sel = st.sidebar.selectbox("Período", list(_PERIODOS.keys()), index=0,
                                    label_visibility="collapsed")
_hoje = datetime.date.today()
if _PERIODOS[_periodo_sel] is None:
    data_inicio, data_fim = None, None
elif _PERIODOS[_periodo_sel] == -1:
    _range_datas = st.sidebar.date_input(
        "Intervalo de datas",
        value=(_hoje - datetime.timedelta(days=30), _hoje),
        max_value=_hoje, format="DD/MM/YYYY",
    )
    if isinstance(_range_datas, (list, tuple)) and len(_range_datas) == 2:
        data_inicio, data_fim = _range_datas
    else:
        data_inicio, data_fim = _range_datas[0], _range_datas[0]
else:
    data_fim    = _hoje
    data_inicio = _hoje - datetime.timedelta(days=_PERIODOS[_periodo_sel])

# 4. Operacional (uso menos frequente)
st.sidebar.markdown("---")
_status_opts    = sorted(df["status"].dropna().unique())
_status_default = ["Ativo"] if "Ativo" in _status_opts else []
status_sel      = st.sidebar.multiselect("⚙️ Status", _status_opts, default=_status_default)
corretor_opts   = sorted(df["corretor"].dropna().unique())
corretor_sel    = st.sidebar.multiselect("👤 Corretor / Imobiliária", corretor_opts)

# ── Filtros aplicados ─────────────────────────────────────────────────────────
dff = df.copy()
if bairros_sel:  dff = dff[dff["bairro"].isin(bairros_sel)]
if quartos_sel:  dff = dff[dff["quartos"].isin(quartos_sel)]
if status_sel:   dff = dff[dff["status"].isin(status_sel)]
if corretor_sel: dff = dff[dff["corretor"].isin(corretor_sel)]
dff = dff[(dff["preco"] >= preco_range[0]) & (dff["preco"] <= preco_range[1])]
dff = dff[dff["area_util"].isna() | dff["area_util"].between(area_range[0], area_range[1])]
if data_inicio is not None:
    _dc = pd.to_datetime(dff["data_cadastro"], errors="coerce").dt.date
    dff = dff[_dc.between(data_inicio, data_fim)]

filtro_label = ", ".join(bairros_sel) if bairros_sel else "Todos os bairros"

# ── Header ────────────────────────────────────────────────────────────────────
st.title("🏠 ImobiFlow — Dashboard Imóveis DF")
st.caption("📦 Fonte: Supabase PostgreSQL — `public.imoveis`")

# ── Abas principais ───────────────────────────────────────────────────────────
tab_mercado, tab_mapa, tab_opor = st.tabs([
    "📊 Mercado",
    "🗺️ Mapa & Tabela",
    "🎯 Oportunidades",
])

# ════════════════════════════════════════════════════════════════════════════════
# ABA 1 — MERCADO
# ════════════════════════════════════════════════════════════════════════════════
with tab_mercado:
    # ── KPIs do mercado ──────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("📋 Total Imóveis", f"{len(dff):,}".replace(",", "."))
    k2.metric("💰 Preço Médio",   f"R$ {_br(dff['preco'].mean())}")
    k3.metric("📐 Área Média",    f"{_br(dff['area_util'].mean())} m²")
    k4.metric("🏷️ R$/m² Médio",   f"R$ {_br(dff['preco_m2'].mean())}")

    _txt_kpi = (
        f"🏠 *Mercado Imobiliário — {filtro_label}*\n\n"
        f"📋 Total de imóveis: {_br(len(dff))}\n"
        f"💰 Preço médio: R$ {_br(dff['preco'].mean())}\n"
        f"📐 Área média: {_br(dff['area_util'].mean())} m²\n"
        f"🏷️ R$/m² médio: R$ {_br(dff['preco_m2'].mean())}\n\n"
        f"📊 Dados atualizados via ImobiFlow Dashboard"
    )
    wa_button(_txt_kpi, "📲 Compartilhar resumo do mercado no WhatsApp")
    st.divider()

    # ── Timeline últimos 7 dias — Novos cadastros × Inativados ──────────────
    st.subheader("📅 Movimentação nos Últimos 7 Dias — DF Imóveis")

    _hoje_tl = pd.Timestamp.today().normalize()
    _7dias   = _hoje_tl - pd.Timedelta(days=6)

    # Novos cadastros por dia (usa data_cadastro = data_cadastro_site)
    _dc = pd.to_datetime(dff["data_cadastro"], errors="coerce")
    _dc_filtrado = _dc[_dc.between(_7dias, _hoje_tl)].dt.date
    _novos_dict = _dc_filtrado.value_counts().to_dict()

    # Inativados por dia (usa dt_inativo)
    _di = pd.to_datetime(df["dt_inativo"], errors="coerce")  # usa df completo (sem filtros)
    _di_filtrado = _di[_di.between(_7dias, _hoje_tl)].dt.date
    _inat_dict = _di_filtrado.value_counts().to_dict()

    # Montar dataframe da timeline com todos os 7 dias
    _datas_range = pd.date_range(_7dias, _hoje_tl, freq="D").date
    _tl = pd.DataFrame({
        "Data": _datas_range,
        "Novos anúncios": [int(_novos_dict.get(d, 0)) for d in _datas_range],
        "Inativados":     [int(_inat_dict.get(d, 0))   for d in _datas_range],
    })
    _tl["Data"] = pd.to_datetime(_tl["Data"])

    fig_tl = px.line(
        _tl.melt(id_vars="Data", var_name="Tipo", value_name="Quantidade"),
        x="Data", y="Quantidade", color="Tipo",
        color_discrete_map={"Novos anúncios": "#2ecc71", "Inativados": "#e74c3c"},
        labels={"Data": "", "Quantidade": "Qtd. Anúncios"},
        markers=True, text="Quantidade",
    )
    fig_tl.update_traces(textposition="top center", line=dict(width=3))
    fig_tl.update_layout(
        height=320, legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center"),
        xaxis_tickformat="%d/%m",
    )
    st.plotly_chart(fig_tl, use_container_width=True, key="fig_timeline", config=CHART_CONFIG)

    _total_novos = int(_tl["Novos anúncios"].sum())
    _total_inat  = int(_tl["Inativados"].sum())
    _txt_tl = (
        f"📅 *Movimentação nos Últimos 7 dias — DF Imóveis*\n📍 {filtro_label}\n\n"
        f"✅ Novos anúncios: {_total_novos}\n"
        f"❌ Inativados: {_total_inat}\n"
        f"📊 Saldo: {_total_novos - _total_inat:+d}\n\n"
        f"📊 ImobiFlow Dashboard"
    )
    chart_actions(fig_tl, "timeline_7dias", _txt_tl)
    st.divider()

    # ── Ranking de Corretores/Imobiliárias — Cadastros × Retiradas ────────
    _col_rank1, _col_rank2 = st.columns(2)

    with _col_rank1:
        st.subheader("🏆 Top Corretores — Mais Cadastros")
        _dc_all = pd.to_datetime(dff["data_cadastro"], errors="coerce")
        _df_novos_corr = dff[_dc_all.between(_7dias, _hoje_tl)].copy()
        if not _df_novos_corr.empty and _df_novos_corr["corretor"].notna().any():
            _rank_novos = (
                _df_novos_corr.groupby("corretor").size()
                .sort_values(ascending=False).head(10).reset_index()
            )
            _rank_novos.columns = ["Corretor/Imobiliária", "Cadastros"]
            fig_rn = px.bar(
                _rank_novos, x="Cadastros", y="Corretor/Imobiliária",
                orientation="h", text="Cadastros",
                color="Cadastros", color_continuous_scale="Greens",
            )
            fig_rn.update_traces(textposition="outside")
            fig_rn.update_layout(
                height=380, showlegend=False, coloraxis_showscale=False,
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_rn, use_container_width=True, key="fig_rank_novos", config=CHART_CONFIG)
            _txt_rn = (
                f"🏆 *Top Corretores — Mais Cadastros (7 dias)*\n📍 {filtro_label}\n\n"
                + "\n".join(f"{i+1}. {r['Corretor/Imobiliária']}: {r['Cadastros']} anúncios"
                            for i, r in _rank_novos.head(5).iterrows())
                + "\n\n📊 ImobiFlow Dashboard"
            )
            chart_actions(fig_rn, "rank_corretores_cadastros", _txt_rn)
        else:
            st.info("Sem dados de cadastros nos últimos 7 dias.")

    with _col_rank2:
        st.subheader("📉 Top Corretores — Mais Retiradas")
        _di_all = pd.to_datetime(df["dt_inativo"], errors="coerce")
        _df_inat_corr = df[_di_all.between(_7dias, _hoje_tl)].copy()
        if not _df_inat_corr.empty and "corretor" in _df_inat_corr.columns and _df_inat_corr["corretor"].notna().any():
            _rank_inat = (
                _df_inat_corr.groupby("corretor").size()
                .sort_values(ascending=False).head(10).reset_index()
            )
            _rank_inat.columns = ["Corretor/Imobiliária", "Retiradas"]
            fig_ri = px.bar(
                _rank_inat, x="Retiradas", y="Corretor/Imobiliária",
                orientation="h", text="Retiradas",
                color="Retiradas", color_continuous_scale="Reds",
            )
            fig_ri.update_traces(textposition="outside")
            fig_ri.update_layout(
                height=380, showlegend=False, coloraxis_showscale=False,
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_ri, use_container_width=True, key="fig_rank_inat", config=CHART_CONFIG)
            _txt_ri = (
                f"📉 *Top Corretores — Mais Retiradas (7 dias)*\n📍 {filtro_label}\n\n"
                + "\n".join(f"{i+1}. {r['Corretor/Imobiliária']}: {r['Retiradas']} retirados"
                            for i, r in _rank_inat.head(5).iterrows())
                + "\n\n📊 ImobiFlow Dashboard"
            )
            chart_actions(fig_ri, "rank_corretores_retiradas", _txt_ri)
        else:
            st.info("Sem dados de retiradas nos últimos 7 dias.")

    st.divider()

    # ── Gráficos de mercado ──────────────────────────────────────────────────
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💵 Preço Médio por Bairro (Top 15)")
        avg = (dff.groupby("bairro")["preco"].mean()
                  .sort_values(ascending=False).head(15).reset_index())
        avg["label"] = avg["preco"].apply(lambda x: f"R$ {_br(x)}")
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
            + "\n".join(f"{i+1}. {r['bairro']}: R$ {_br(r['preco'])}" for i, r in _top5.iterrows())
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
            f"💰 Mínimo: R$ {_br(dff['preco'].min())}\n"
            f"💰 25%: R$ {_br(_p25)}\n"
            f"💰 Mediana: R$ {_br(dff['preco'].median())}\n"
            f"💰 75%: R$ {_br(_p75)}\n"
            f"💰 Máximo: R$ {_br(dff['preco'].max())}\n\n"
            f"📊 ImobiFlow Dashboard"
        )
        chart_actions(fig, "distribuicao_precos", _txt)

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
            f"📐 Área mínima: {_br(dff['area_util'].min())} m²\n"
            f"📐 Área média: {_br(dff['area_util'].mean())} m²\n"
            f"📐 Área máxima: {_br(dff['area_util'].max())} m²\n\n"
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

    col5, col6 = st.columns(2)

    with col5:
        st.subheader("🏷️ R$/m² Médio por Bairro (Top 15)")
        avg_m2 = (dff.groupby("bairro")["preco_m2"].mean()
                     .sort_values(ascending=False).head(15).reset_index())
        avg_m2["label"] = avg_m2["preco_m2"].apply(lambda x: f"R$ {_br(x)}")
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
            + "\n".join(f"{i+1}. {r['bairro']}: R$ {_br(r['preco_m2'])}/m²"
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
                    .groupby("quartos")["preco"].median().sort_index())
        _txt = (
            f"📦 *Preço Mediano por Nº de Quartos — {filtro_label}*\n\n"
            + "\n".join(f"  {int(q)} quartos: R$ {_br(v)}" for q, v in _medians.items())
            + "\n\n📊 ImobiFlow Dashboard"
        )
        chart_actions(fig, "boxplot_preco_quartos", _txt)

# ════════════════════════════════════════════════════════════════════════════════
# ABA 2 — MAPA & TABELA
# Mapa no topo; clique em ponto filtra a tabela abaixo
# ════════════════════════════════════════════════════════════════════════════════
with tab_mapa:
    # Prepara df para o mapa preservando o índice original de dff
    df_map = dff.dropna(subset=["lat", "lon"]).copy()
    df_map = df_map[(df_map["lat"].between(-16.5, -15.4)) &
                    (df_map["lon"].between(-48.5, -47.2))]
    df_map["area_util_sz"] = df_map["area_util"].fillna(0).clip(lower=0)
    # Guarda índice original para vincular clique → tabela
    df_map = df_map.reset_index()          # coluna "index" = índice original do dff

    if df_map.empty:
        st.info("Nenhum imóvel com coordenadas nos filtros atuais.")
    else:
        fig_map = px.scatter_mapbox(
            df_map, lat="lat", lon="lon",
            color="preco", size="area_util_sz", size_max=20,
            hover_data=["bairro", "preco", "area_util", "quartos", "preco_m2", "endereco"],
            custom_data=["index"],          # ← índice original → usado no filtro da tabela
            color_continuous_scale="RdYlGn_r",
            zoom=10.5, height=500, mapbox_style="carto-positron",
            labels={"preco": "Preço (R$)"}
        )
        fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

        # Contador de limpeza: incrementar muda a key do gráfico → reseta seleção
        if "mapa_clear" not in st.session_state:
            st.session_state.mapa_clear = 0

        # on_select="rerun" → re-executa o app quando o usuário clica em um ponto
        # key dinâmica → quando mapa_clear incrementa, widget é recriado sem seleção
        mapa_sel = st.plotly_chart(
            fig_map, use_container_width=True,
            key=f"fig_mapa_{st.session_state.mapa_clear}",
            config=MAP_CONFIG, on_select="rerun", selection_mode="points"
        )

        _txt_mapa = (
            f"🗺️ *Mapa de Imóveis — {filtro_label}*\n\n"
            f"📍 {len(df_map)} imóveis mapeados no Distrito Federal\n"
            f"💰 Preço médio: R$ {_br(df_map['preco'].mean())}\n"
            f"🏷️ R$/m² médio: R$ {_br(df_map['preco_m2'].mean())}\n\n"
            f"📊 ImobiFlow Dashboard"
        )
        chart_actions(fig_map, "mapa_imoveis", _txt_mapa,
                      "📲 Compartilhar dados do mapa no WhatsApp")

        # ── Tabela vinculada ao clique no mapa ────────────────────────────────
        st.divider()

        # Detecta se há pontos selecionados no mapa
        _sel_pts = (mapa_sel.selection.points
                    if mapa_sel and hasattr(mapa_sel, "selection")
                       and mapa_sel.selection and mapa_sel.selection.points
                    else [])

        if _sel_pts:
            _orig_idx = [int(p["customdata"][0]) for p in _sel_pts
                         if p.get("customdata")]
            df_tabela = dff.loc[_orig_idx] if _orig_idx else dff
            _col_lbl, _col_btn = st.columns([3, 1])
            _col_lbl.markdown(
                f"**📍 {len(_sel_pts)} imóvel(is) selecionado(s) no mapa**"
            )
            with _col_btn:
                if st.button("✖ Limpar seleção", use_container_width=True):
                    st.session_state.mapa_clear += 1  # nova key → widget recriado sem seleção
                    st.rerun()
        else:
            df_tabela = dff
            st.subheader(f"📋 Todos os imóveis ({len(dff):,})")

        montar_grid(df_tabela, key_prefix="mapa_tab",
                    cols_base=["bairro", "preco", "area_util", "preco_m2",
                               "quartos", "banheiros", "endereco", "corretor", "url"],
                    altura=460)

        # WhatsApp melhores ofertas da seleção atual
        _melhores = df_tabela.dropna(subset=["preco_m2", "url"]).sort_values("preco_m2").head(5)
        if not _melhores.empty:
            _linhas = "\n".join(
                f"{i+1}. {r['bairro']} | {int(r['quartos']) if pd.notna(r['quartos']) else '?'}q "
                f"| R$ {_br(r['preco'])} | R$/m² {_br(r['preco_m2'])}\n   🔗 {r['url']}"
                for i, (_, r) in enumerate(_melhores.iterrows())
            )
            wa_button(
                f"🏆 *Melhores Ofertas por R$/m² — {filtro_label}*\n\n"
                f"{_linhas}\n\n📊 ImobiFlow Dashboard",
                "📲 Compartilhar melhores ofertas no WhatsApp"
            )

# ════════════════════════════════════════════════════════════════════════════════
# ABA 3 — OPORTUNIDADES  (sub-abas: Captação | Venda)
# ════════════════════════════════════════════════════════════════════════════════
with tab_opor:
    sub_capt, sub_venda = st.tabs(["🎯 Captação", "💡 Venda"])

    # ── Sub-aba: Captação ──────────────────────────────────────────────────────
    with sub_capt:
        st.subheader("🎯 Oportunidade de Captação")
        st.caption(
            "Imóveis com **endereço completo** identificado (Bloco para aptos / Conjunto para casas) — "
            "ideais para abordagem direta de captação."
        )

        _RE_BLOCO    = re.compile(r'\b(bl\.?|bloco)\s*[a-z0-9]+', re.IGNORECASE)
        _RE_CONJUNTO = re.compile(r'\b(conj\.?|conjunto)\s*[0-9a-z]+', re.IGNORECASE)

        def _match_campos(row, regex):
            for campo in ("titulo_vitrine", "descricao", "endereco"):
                val = row.get(campo) or ""
                if regex.search(str(val)):
                    return True
            return False

        def _extrair_referencia(row, regex):
            for campo in ("endereco", "titulo_vitrine", "descricao"):
                val = row.get(campo) or ""
                m = regex.search(str(val))
                if m:
                    start = m.start()
                    return str(val)[max(0, start - 5):start + 30].strip()
            return ""

        df_capt      = dff.copy()
        mask_apto    = (df_capt["tipo_imovel"] == "Apartamento") & \
                       df_capt.apply(lambda r: _match_campos(r, _RE_BLOCO), axis=1)
        mask_casa    = (df_capt["tipo_imovel"] == "Casa") & \
                       df_capt.apply(lambda r: _match_campos(r, _RE_CONJUNTO), axis=1)
        df_apto_capt = df_capt[mask_apto].copy()
        df_casa_capt = df_capt[mask_casa].copy()
        df_apto_capt["Bloco"]    = df_apto_capt.apply(lambda r: _extrair_referencia(r, _RE_BLOCO), axis=1)
        df_casa_capt["Conjunto"] = df_casa_capt.apply(lambda r: _extrair_referencia(r, _RE_CONJUNTO), axis=1)

        _tipo_apto, _tipo_casa = st.tabs([
            f"🏢 Apartamentos ({len(df_apto_capt)})",
            f"🏠 Casas ({len(df_casa_capt)})",
        ])

        _COLS_CAPT = ["bairro", "preco", "area_util", "preco_m2",
                      "quartos", "banheiros", "endereco", "corretor", "url"]

        with _tipo_apto:
            if df_apto_capt.empty:
                st.info("Nenhum imóvel encontrado com os filtros atuais.")
            else:
                montar_grid(df_apto_capt, "capt_apto", _COLS_CAPT, col_extra="Bloco", altura=460)
                _top5 = df_apto_capt.dropna(subset=["preco_m2"]).sort_values("preco_m2").head(5)
                if not _top5.empty:
                    _linhas = "\n".join(
                        f"{i+1}. {r['bairro']} | {r.get('Bloco','')} | "
                        f"R$ {_br(r['preco'])} | {r['url']}"
                        for i, (_, r) in enumerate(_top5.iterrows())
                    )
                    wa_button(
                        f"🎯 *Oportunidades de Captação — Apartamentos*\n📍 {filtro_label}\n\n"
                        f"{_linhas}\n\n📊 ImobiFlow Dashboard",
                        "📲 Compartilhar oportunidades (Aptos) no WhatsApp"
                    )

        with _tipo_casa:
            if df_casa_capt.empty:
                st.info("Nenhum imóvel encontrado com os filtros atuais.")
            else:
                montar_grid(df_casa_capt, "capt_casa", _COLS_CAPT, col_extra="Conjunto", altura=460)
                _top5 = df_casa_capt.dropna(subset=["preco_m2"]).sort_values("preco_m2").head(5)
                if not _top5.empty:
                    _linhas = "\n".join(
                        f"{i+1}. {r['bairro']} | {r.get('Conjunto','')} | "
                        f"R$ {_br(r['preco'])} | {r['url']}"
                        for i, (_, r) in enumerate(_top5.iterrows())
                    )
                    wa_button(
                        f"🎯 *Oportunidades de Captação — Casas*\n📍 {filtro_label}\n\n"
                        f"{_linhas}\n\n📊 ImobiFlow Dashboard",
                        "📲 Compartilhar oportunidades (Casas) no WhatsApp"
                    )

    # ── Sub-aba: Venda ─────────────────────────────────────────────────────────
    with sub_venda:
        st.subheader("💡 Oportunidade de Venda")
        st.caption(
            "Imóveis com **R$/m² abaixo ou próximo da média do bairro** — "
            "potencial de valorização ou venda rápida por preço competitivo."
        )

        _col_thr, _ = st.columns([2, 5])
        with _col_thr:
            _threshold = st.slider(
                "Máximo acima da média do bairro (%)",
                min_value=-50, max_value=20, value=0, step=5, format="%d%%",
                help="0% = apenas abaixo da média | 10% = inclui até 10% acima da média"
            )

        _media_bairro = (
            df.dropna(subset=["preco_m2", "bairro"])
            .groupby("bairro")["preco_m2"].mean()
            .rename("media_m2_bairro")
        )
        df_venda = dff.dropna(subset=["preco_m2"]).copy()
        df_venda = df_venda.join(_media_bairro, on="bairro")
        df_venda["var_vs_media_pct"] = (
            (df_venda["preco_m2"] - df_venda["media_m2_bairro"])
            / df_venda["media_m2_bairro"] * 100
        ).round(1)
        df_venda = df_venda[df_venda["var_vs_media_pct"] <= _threshold].sort_values("var_vs_media_pct")

        _kv1, _kv2, _kv3, _ = st.columns([1, 1, 1, 3])
        _kv1.metric("📋 Oportunidades", f"{len(df_venda):,}")
        if not df_venda.empty:
            _kv2.metric("🏷️ Maior desconto", f"{df_venda['var_vs_media_pct'].min():+.1f}%")
            _kv3.metric("💰 Menor R$/m²",    f"R$ {_br(df_venda['preco_m2'].min())}")

        st.write("")

        if df_venda.empty:
            st.info("Nenhum imóvel encontrado com os critérios atuais.")
        else:
            _cols_v = ["bairro", "preco", "area_util", "preco_m2",
                       "media_m2_bairro", "var_vs_media_pct",
                       "quartos", "banheiros", "endereco", "corretor", "url"]
            montar_grid(df_venda, "venda", _cols_v, altura=520)

            _top5v    = df_venda.head(5)
            _linhas_v = "\n".join(
                f"{i+1}. {r['bairro']} | R$/m² {_br(r['preco_m2'])} "
                f"({r['var_vs_media_pct']:+.1f}% vs média) | R$ {_br(r['preco'])}\n   🔗 {r['url']}"
                for i, (_, r) in enumerate(_top5v.iterrows())
            )
            wa_button(
                f"💡 *Oportunidades de Venda — {filtro_label}*\n"
                f"_(imóveis abaixo ou na média de R$/m² do bairro)_\n\n"
                f"{_linhas_v}\n\n📊 ImobiFlow Dashboard",
                "📲 Compartilhar oportunidades de venda no WhatsApp"
            )

st.divider()
st.caption("🚀 ImobiFlow © 2026 | Supabase PostgreSQL")
