import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
from src.core.storage import storage

st.set_page_config(
    page_title="GOVBR Monitor | Inteligência Anticorrupção",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@300;400;500;600&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; background-color: #0A0E1A; color: #F1F5F9; }
.main { background-color: #0A0E1A; }
.block-container { padding: 2rem; }
h1, h2, h3 { font-family: 'JetBrains Mono', monospace; color: #F1F5F9; }
.metric-card { background: #111827; border: 1px solid #1E2A3A; border-radius: 8px; padding: 1.2rem 1.5rem; margin-bottom: 1rem; }
.metric-value { font-family: 'JetBrains Mono', monospace; font-size: 2.2rem; font-weight: 700; line-height: 1; margin-bottom: 0.3rem; }
.metric-label { font-size: 0.75rem; color: #64748B; text-transform: uppercase; letter-spacing: 0.1em; }
.alert-card { background: #111827; border-left: 4px solid; border-radius: 0 8px 8px 0; padding: 1rem 1.2rem; margin-bottom: 0.75rem; }
.alert-critico { border-color: #EF4444; }
.alert-gravissimo { border-color: #7C3AED; }
.alert-historico { border-color: #F59E0B; }
.alert-fracionamento { border-color: #06B6D4; }
.alert-empresa { font-family: 'JetBrains Mono', monospace; font-size: 0.95rem; font-weight: 700; color: #F1F5F9; }
.alert-meta { font-size: 0.78rem; color: #64748B; margin-top: 0.3rem; }
.score-bar-bg { background: #1E2A3A; border-radius: 4px; height: 6px; margin-top: 0.5rem; }
.badge { display: inline-block; font-family: 'JetBrains Mono', monospace; font-size: 0.65rem; font-weight: 700; padding: 0.2rem 0.5rem; border-radius: 4px; text-transform: uppercase; letter-spacing: 0.05em; }
.badge-critico { background: #7F1D1D; color: #FCA5A5; }
.badge-gravissimo { background: #4C1D95; color: #C4B5FD; }
.badge-historico { background: #78350F; color: #FDE68A; }
.badge-fracionamento { background: #164E63; color: #67E8F9; }
.badge-maximo { background: #7F1D1D; color: #FCA5A5; }
.badge-alto { background: #7C2D12; color: #FDBA74; }
.badge-medio { background: #713F12; color: #FDE68A; }
.badge-baixo { background: #14532D; color: #86EFAC; }
section[data-testid="stSidebar"] { background-color: #0D1117; border-right: 1px solid #1E2A3A; }
</style>
""", unsafe_allow_html=True)


def badge_risco(classificacao: str) -> str:
    if "GRAVISSIMO" in classificacao:
        return '<span class="badge badge-gravissimo">⚠ gravíssimo</span>'
    if "CRITICO" in classificacao:
        return '<span class="badge badge-critico">🔴 crítico</span>'
    return '<span class="badge badge-historico">🟡 histórico</span>'

def badge_nivel(nivel: str) -> str:
    mapa = {"MAXIMO": "badge-maximo", "ALTO": "badge-alto", "MEDIO": "badge-medio", "BAIXO": "badge-baixo"}
    cls = mapa.get(nivel, "badge-baixo")
    return f'<span class="badge {cls}">{nivel}</span>'

def score_bar(score: int, max_score: int = 100) -> str:
    pct = min(score / max_score * 100, 100)
    cor = "#EF4444" if pct >= 80 else "#F97316" if pct >= 60 else "#F59E0B" if pct >= 40 else "#10B981"
    return f'<div class="score-bar-bg"><div style="width:{pct}%;background:{cor};height:6px;border-radius:4px;"></div></div>'

def plotly_dark(fig):
    fig.update_layout(
        paper_bgcolor="#111827", plot_bgcolor="#111827", font_color="#F1F5F9",
        margin=dict(l=0, r=0, t=20, b=0),
        xaxis=dict(showgrid=True, gridcolor="#1E2A3A"),
        yaxis=dict(showgrid=True, gridcolor="#1E2A3A"),
    )
    return fig


@st.cache_data(ttl=300)
def carregar_dados():
    return {
        "alertas":       storage.download_parquet("gold/analytics_alertas_corrupcao.parquet"),
        "sancoes":       storage.download_parquet("gold/fact_ceis_sancoes.parquet"),
        "orgaos_sanc":   storage.download_parquet("gold/dm_sancoes_por_orgao.parquet"),
        "testa":         storage.download_parquet("gold/analytics_testa_ferro.parquet"),
        "fracionamento": storage.download_parquet("gold/analytics_fracionamento.parquet"),
        "concentracao":  storage.download_parquet("gold/analytics_concentracao.parquet"),
        "orgaos_risco":  storage.download_parquet("gold/analytics_orgaos_risco.parquet"),
        "temporal":      storage.download_parquet("gold/analytics_temporal.parquet"),
        "empenho":       storage.download_parquet("gold/analytics_empenho_direto.parquet"),
    }

with st.spinner("Carregando dados..."):
    dados = carregar_dados()

df_alertas      = dados["alertas"]
df_sancoes      = dados["sancoes"]
df_orgaos_sanc  = dados["orgaos_sanc"]
df_testa        = dados["testa"]
df_frac         = dados["fracionamento"]
df_conc         = dados["concentracao"]
df_orgaos_risco = dados["orgaos_risco"]
df_temporal     = dados["temporal"]
df_empenho      = dados["empenho"]

if df_alertas is None:
    st.error("Dados não encontrados. Execute: `python -m src.main`")
    st.stop()

# --- Sidebar ---
with st.sidebar:
    st.markdown("### 🔍 GOVBR Monitor")
    st.markdown('<p style="color:#64748B;font-size:0.75rem;">Inteligência Anticorrupção</p>', unsafe_allow_html=True)
    st.divider()
    pagina = st.radio("", [
        "Painel Geral",
        "Alertas",
        "Fracionamento",
        "Concentração",
        "Órgãos de Risco",
        "Evolução Temporal",
        "Sanções",
        "Testa de Ferro",
    ], label_visibility="collapsed")
    st.divider()
    criticos = df_alertas.filter(pl.col("classificacao_risco").str.contains("CRITICO")).height
    frac_count = df_frac.height if df_frac is not None else 0
    st.markdown(f'<div class="metric-label">alertas críticos</div><div style="font-family:JetBrains Mono;font-size:1.8rem;color:#EF4444;font-weight:700">{criticos}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="metric-label" style="margin-top:0.5rem">suspeitas fracionamento</div><div style="font-family:JetBrains Mono;font-size:1.8rem;color:#06B6D4;font-weight:700">{frac_count}</div>', unsafe_allow_html=True)
    st.divider()
    st.markdown(f'<p style="color:#64748B;font-size:0.7rem;">Atualizado: {date.today().strftime("%d/%m/%Y")}</p>', unsafe_allow_html=True)


# ============================================================
# PAINEL GERAL
# ============================================================
if pagina == "Painel Geral":
    st.markdown("# GOVBR Monitor")
    st.markdown('<p style="color:#64748B;font-size:0.9rem;margin-top:-1rem;margin-bottom:2rem;">Sistema de Inteligência Anticorrupção — Dados Públicos Brasileiros</p>', unsafe_allow_html=True)

    total       = df_alertas.height
    criticos    = df_alertas.filter(pl.col("classificacao_risco").str.contains("CRITICO")).height
    gravissimos = df_alertas.filter(pl.col("classificacao_risco").str.contains("GRAVISSIMO")).height
    valor_total = df_alertas["valor_global_contrato"].sum() or 0
    frac_count  = df_frac.height if df_frac is not None else 0
    conc_top    = df_conc.sort("valor_total", descending=True).row(0, named=True) if df_conc is not None and df_conc.height > 0 else None

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#EF4444">{criticos}</div><div class="metric-label">críticos</div></div>', unsafe_allow_html=True)
    with col2:
        st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#7C3AED">{gravissimos}</div><div class="metric-label">gravíssimos</div></div>', unsafe_allow_html=True)
    with col3:
        st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#06B6D4">{frac_count}</div><div class="metric-label">fracionamentos</div></div>', unsafe_allow_html=True)
    with col4:
        st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#3B82F6">{total}</div><div class="metric-label">total alertas</div></div>', unsafe_allow_html=True)
    with col5:
        valor_fmt = f"R$ {valor_total:,.0f}".replace(",", ".")
        st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#F59E0B;font-size:1.3rem">{valor_fmt}</div><div class="metric-label">valor suspeito</div></div>', unsafe_allow_html=True)

    st.markdown("---")
    col_a, col_b = st.columns(2)

    with col_a:
        st.markdown("#### Distribuição de Risco")
        contagem = df_alertas.group_by("classificacao_risco").agg(pl.len().alias("total")).sort("total", descending=True)
        fig = px.bar(contagem.to_pandas(), x="classificacao_risco", y="total", color="classificacao_risco",
            color_discrete_map={
                "CRITICO: CONTRATACAO DE EMPRESA IMPEDIDA": "#EF4444",
                "GRAVISSIMO: EMPRESA PUNIDA POR CORRUPCAO CONTRATANDO": "#7C3AED",
                "HISTORICO: FORNECEDOR COM SANCOES CORRELATAS": "#F59E0B",
            })
        fig.update_layout(showlegend=False)
        st.plotly_chart(plotly_dark(fig), use_container_width=True)

    with col_b:
        st.markdown("#### Evolução de Contratos")
        if df_temporal is not None and df_temporal.height > 0:
            df_temp_pd = df_temporal.to_pandas()
            fig2 = go.Figure()
            fig2.add_trace(go.Scatter(
                x=df_temp_pd["data_assinatura"], y=df_temp_pd["total_contratos"],
                mode="lines+markers", name="Contratos",
                line=dict(color="#3B82F6", width=2),
                marker=dict(size=4)
            ))
            if "total_alertas" in df_temp_pd.columns:
                fig2.add_trace(go.Scatter(
                    x=df_temp_pd["data_assinatura"], y=df_temp_pd["total_alertas"],
                    mode="lines+markers", name="Alertas",
                    line=dict(color="#EF4444", width=2, dash="dot"),
                    marker=dict(size=4)
                ))
            st.plotly_chart(plotly_dark(fig2), use_container_width=True)

    if conc_top:
        st.markdown("---")
        st.markdown("#### ⚠️ Maior Concentração Detectada")
        pct = conc_top.get("pct_valor_total", 0)
        st.markdown(f"""
        <div class="alert-card alert-fracionamento">
            <div class="alert-empresa">{conc_top['nome_fornecedor']}</div>
            <div class="alert-meta">
                {conc_top['total_contratos']} contrato(s) · 
                R$ {conc_top['valor_total']:,.2f} · 
                {pct}% do valor total da amostra
            </div>
        </div>
        """, unsafe_allow_html=True)


# ============================================================
# ALERTAS
# ============================================================
elif pagina == "Alertas":
    st.markdown("# Alertas Detectados")
    st.markdown(f'<p style="color:#64748B;font-size:0.9rem;margin-top:-1rem;margin-bottom:2rem;">{df_alertas.height} ocorrências identificadas</p>', unsafe_allow_html=True)

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        niveis = ["Todos"] + (df_alertas["nivel_risco"].unique().to_list() if "nivel_risco" in df_alertas.columns else [])
        filtro_nivel = st.selectbox("Nível de risco", niveis)
    with col_f2:
        filtro_busca = st.text_input("Buscar empresa", placeholder="Nome ou CNPJ...")

    df_view = df_alertas
    if filtro_nivel != "Todos":
        df_view = df_view.filter(pl.col("nivel_risco") == filtro_nivel)
    if filtro_busca:
        df_view = df_view.filter(
            pl.col("nome_fornecedor").str.to_uppercase().str.contains(filtro_busca.upper()) |
            pl.col("documento_fornecedor_limpo").str.contains(filtro_busca)
        )

    st.markdown(f'<p style="color:#64748B;font-size:0.8rem;">{df_view.height} resultado(s)</p>', unsafe_allow_html=True)

    sort_col = "score_risco_total" if "score_risco_total" in df_view.columns else "classificacao_risco"
    for row in df_view.sort(sort_col, descending=True).iter_rows(named=True):
        classe = "alert-critico" if "CRITICO" in row["classificacao_risco"] else "alert-gravissimo" if "GRAVISSIMO" in row["classificacao_risco"] else "alert-historico"
        score  = row.get("score_risco_total", 0) or 0
        nivel  = row.get("nivel_risco", "—") or "—"
        socios = row.get("socios", "—") or "—"
        abertura = row.get("data_inicio_atividade", "—")
        capital  = row.get("capital_social", 0) or 0

        st.markdown(f"""
        <div class="alert-card {classe}">
            <div style="display:flex;justify-content:space-between;align-items:flex-start">
                <div>
                    <div class="alert-empresa">{row['nome_fornecedor']}</div>
                    <div class="alert-meta">CNPJ: {row['documento_fornecedor_limpo']} · {row.get('municipio','—')}/{row.get('uf','—')}</div>
                </div>
                <div style="text-align:right">{badge_risco(row['classificacao_risco'])}<div style="margin-top:0.3rem">{badge_nivel(nivel)}</div></div>
            </div>
            {score_bar(score)}
            <div style="margin-top:0.8rem;display:grid;grid-template-columns:1fr 1fr 1fr;gap:0.5rem">
                <div><div class="metric-label">órgão contratante</div><div style="font-size:0.8rem;color:#CBD5E1">{row['nome_orgao_comprador']}</div></div>
                <div><div class="metric-label">valor do contrato</div><div style="font-size:0.8rem;color:#CBD5E1">R$ {row['valor_global_contrato']:,.2f}</div></div>
                <div><div class="metric-label">data assinatura</div><div style="font-size:0.8rem;color:#CBD5E1">{row['data_assinatura']}</div></div>
                <div><div class="metric-label">sócios</div><div style="font-size:0.8rem;color:#CBD5E1">{socios}</div></div>
                <div><div class="metric-label">abertura empresa</div><div style="font-size:0.8rem;color:#CBD5E1">{abertura}</div></div>
                <div><div class="metric-label">capital social</div><div style="font-size:0.8rem;color:#CBD5E1">R$ {capital:,.0f}</div></div>
            </div>
            <div style="margin-top:0.8rem">
                <div class="metric-label">sanção · {row['orgao_sancionador']}</div>
                <div style="font-size:0.8rem;color:#CBD5E1">{row['tipo_sancao']}</div>
            </div>
            <div style="margin-top:0.5rem;display:flex;align-items:center;gap:1rem">
                <div><div class="metric-label">score de risco</div>
                <div style="font-family:'JetBrains Mono',monospace;font-size:1.1rem;color:#F1F5F9;font-weight:700">{score} <span style="color:#64748B;font-size:0.75rem">/ 100</span></div></div>
            </div>
        </div>
        """, unsafe_allow_html=True)


# ============================================================
# FRACIONAMENTO
# ============================================================
elif pagina == "Fracionamento":
    st.markdown("# Suspeita de Fracionamento")
    st.markdown('<p style="color:#64748B;font-size:0.9rem;margin-top:-1rem;margin-bottom:2rem;">Mesmo fornecedor, mesmo órgão, múltiplos contratos abaixo do limite de licitação</p>', unsafe_allow_html=True)

    if df_frac is None or df_frac.height == 0:
        st.info("Nenhum padrão detectado na amostragem atual.")
    else:
        st.warning(f"{df_frac.height} padrão(ões) suspeito(s) detectado(s)")
        for row in df_frac.sort("total_contratos", descending=True).iter_rows(named=True):
            st.markdown(f"""
            <div class="alert-card alert-fracionamento">
                <div style="display:flex;justify-content:space-between">
                    <div>
                        <div class="alert-empresa">{row['nome_fornecedor']}</div>
                        <div class="alert-meta">CNPJ: {row['documento_fornecedor_limpo']}</div>
                    </div>
                    <span class="badge badge-fracionamento">fracionamento</span>
                </div>
                <div style="margin-top:0.8rem;display:grid;grid-template-columns:1fr 1fr 1fr 1fr;gap:0.5rem">
                    <div><div class="metric-label">órgão</div><div style="font-size:0.8rem;color:#CBD5E1">{row['nome_orgao_comprador']}</div></div>
                    <div><div class="metric-label">nº de contratos</div><div style="font-family:'JetBrains Mono',monospace;font-size:1.1rem;color:#06B6D4;font-weight:700">{row['total_contratos']}</div></div>
                    <div><div class="metric-label">valor total</div><div style="font-size:0.8rem;color:#CBD5E1">R$ {row['valor_total']:,.2f}</div></div>
                    <div><div class="metric-label">valor médio</div><div style="font-size:0.8rem;color:#CBD5E1">R$ {row['valor_medio']:,.2f}</div></div>
                    <div><div class="metric-label">primeiro contrato</div><div style="font-size:0.8rem;color:#CBD5E1">{row['primeiro_contrato']}</div></div>
                    <div><div class="metric-label">último contrato</div><div style="font-size:0.8rem;color:#CBD5E1">{row['ultimo_contrato']}</div></div>
                    <div><div class="metric-label">múltiplos do limite</div><div style="font-family:'JetBrains Mono',monospace;font-size:1.1rem;color:#F59E0B;font-weight:700">{row['multiplos_do_limite']}x</div></div>
                </div>
            </div>
            """, unsafe_allow_html=True)


# ============================================================
# CONCENTRAÇÃO
# ============================================================
elif pagina == "Concentração":
    st.markdown("# Concentração de Contratos")
    st.markdown('<p style="color:#64748B;font-size:0.9rem;margin-top:-1rem;margin-bottom:2rem;">Fornecedores com maior participação no volume total de contratos</p>', unsafe_allow_html=True)

    if df_conc is None or df_conc.height == 0:
        st.info("Sem dados de concentração.")
    else:
        col1, col2, col3 = st.columns(3)
        top = df_conc.sort("valor_total", descending=True).row(0, named=True)
        with col1:
            st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#3B82F6">{df_conc.height}</div><div class="metric-label">fornecedores únicos</div></div>', unsafe_allow_html=True)
        with col2:
            st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#F59E0B;font-size:1.3rem">R$ {top["valor_total"]:,.0f}</div><div class="metric-label">maior concentração</div></div>', unsafe_allow_html=True)
        with col3:
            st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#EF4444">{top["pct_valor_total"]}%</div><div class="metric-label">% do valor total</div></div>', unsafe_allow_html=True)

        st.markdown("#### Top 20 por Valor Total")
        fig = px.bar(
            df_conc.sort("valor_total", descending=True).head(20).to_pandas(),
            x="valor_total", y="nome_fornecedor", orientation="h",
            color="total_contratos", color_continuous_scale=["#1E3A5F", "#3B82F6"],
        )
        fig.update_layout(showlegend=False, coloraxis_showscale=False, height=500)
        st.plotly_chart(plotly_dark(fig), use_container_width=True)

        st.markdown("#### Tabela Completa")
        st.dataframe(
            df_conc.sort("valor_total", descending=True)
            .select(["nome_fornecedor", "documento_fornecedor_limpo", "total_contratos", "orgaos_distintos", "valor_total", "pct_valor_total"])
            .to_pandas(),
            use_container_width=True, hide_index=True
        )


# ============================================================
# ÓRGÃOS DE RISCO
# ============================================================
elif pagina == "Órgãos de Risco":
    st.markdown("# Órgãos com Maior Risco")
    st.markdown('<p style="color:#64748B;font-size:0.9rem;margin-top:-1rem;margin-bottom:2rem;">Instituições com maior incidência de alertas nos contratos</p>', unsafe_allow_html=True)

    if df_orgaos_risco is None or df_orgaos_risco.height == 0:
        st.info("Sem dados de risco por órgão.")
    else:
        fig = px.scatter(
            df_orgaos_risco.to_pandas(),
            x="total_contratos_orgao", y="total_alertas",
            size="valor_total_suspeito", color="pct_contratos_suspeitos",
            color_continuous_scale=["#1E3A5F", "#EF4444"],
            hover_name="nome_orgao_comprador",
            labels={
                "total_contratos_orgao": "Total de Contratos",
                "total_alertas": "Total de Alertas",
                "pct_contratos_suspeitos": "% Suspeitos"
            }
        )
        fig.update_layout(height=450, coloraxis_showscale=True)
        st.plotly_chart(plotly_dark(fig), use_container_width=True)

        st.markdown("#### Ranking de Risco")
        st.dataframe(
            df_orgaos_risco.sort("pct_contratos_suspeitos", descending=True)
            .select(["nome_orgao_comprador", "total_alertas", "alertas_criticos", "total_contratos_orgao", "pct_contratos_suspeitos", "valor_total_suspeito"])
            .to_pandas(),
            use_container_width=True, hide_index=True
        )


# ============================================================
# EVOLUÇÃO TEMPORAL
# ============================================================
elif pagina == "Evolução Temporal":
    st.markdown("# Evolução Temporal")
    st.markdown('<p style="color:#64748B;font-size:0.9rem;margin-top:-1rem;margin-bottom:2rem;">Distribuição de contratos e alertas ao longo do tempo</p>', unsafe_allow_html=True)

    if df_temporal is None or df_temporal.height == 0:
        st.info("Sem dados temporais.")
    else:
        df_temp_pd = df_temporal.to_pandas()

        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df_temp_pd["data_assinatura"], y=df_temp_pd["total_contratos"],
            name="Contratos", marker_color="#3B82F6", opacity=0.7
        ))
        if "total_alertas" in df_temp_pd.columns:
            fig.add_trace(go.Scatter(
                x=df_temp_pd["data_assinatura"], y=df_temp_pd["total_alertas"],
                name="Alertas", mode="lines+markers",
                line=dict(color="#EF4444", width=2),
                yaxis="y2"
            ))
        fig.update_layout(
            yaxis2=dict(overlaying="y", side="right", showgrid=False, color="#EF4444"),
            legend=dict(bgcolor="#111827", bordercolor="#1E2A3A"),
            height=400,
            barmode="overlay"
        )
        st.plotly_chart(plotly_dark(fig), use_container_width=True)

        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#3B82F6">{df_temporal["total_contratos"].sum()}</div><div class="metric-label">total contratos</div></div>', unsafe_allow_html=True)
        with col2:
            st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#F59E0B">R$ {df_temporal["valor_total"].sum():,.0f}</div><div class="metric-label">valor total</div></div>', unsafe_allow_html=True)
        with col3:
            st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#64748B">{df_temporal.height}</div><div class="metric-label">dias monitorados</div></div>', unsafe_allow_html=True)


# ============================================================
# SANÇÕES
# ============================================================
elif pagina == "Sanções":
    st.markdown("# Base de Sanções CEIS")
    st.markdown(f'<p style="color:#64748B;font-size:0.9rem;margin-top:-1rem;margin-bottom:2rem;">{df_sancoes.height if df_sancoes is not None else 0} registros</p>', unsafe_allow_html=True)

    if df_sancoes is not None:
        ativas = df_sancoes.filter(pl.col("status_sancao") == "ATIVA").height
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#EF4444">{ativas}</div><div class="metric-label">sanções ativas</div></div>', unsafe_allow_html=True)
        with col2:
            st.markdown(f'<div class="metric-card"><div class="metric-value" style="color:#64748B">{df_sancoes.height - ativas}</div><div class="metric-label">sanções expiradas</div></div>', unsafe_allow_html=True)

        if df_orgaos_sanc is not None:
            st.markdown("#### Top Órgãos Sancionadores")
            fig = px.bar(
                df_orgaos_sanc.head(15).to_pandas(),
                x="total_sancoes", y="orgao_sancionador", orientation="h",
                color="sancoes_ativas", color_continuous_scale=["#1E2A3A", "#EF4444"],
            )
            fig.update_layout(coloraxis_showscale=False, height=400)
            st.plotly_chart(plotly_dark(fig), use_container_width=True)

        colunas = ["nome_empresa_ou_pessoa", "documento_limpo", "tipo_sancao", "data_inicio", "data_fim", "status_sancao", "orgao_sancionador"]
        st.dataframe(df_sancoes.select([c for c in colunas if c in df_sancoes.columns]).to_pandas(), use_container_width=True, hide_index=True)


# ============================================================
# TESTA DE FERRO
# ============================================================
elif pagina == "Testa de Ferro":
    st.markdown("# Detecção de Testa de Ferro")
    st.markdown('<p style="color:#64748B;font-size:0.9rem;margin-top:-1rem;margin-bottom:2rem;">Sócios de empresas impedidas contratando via outras empresas</p>', unsafe_allow_html=True)

    if df_testa is None or df_testa.height == 0:
        st.markdown("""
        <div class="metric-card" style="text-align:center;padding:3rem">
            <div style="font-family:'JetBrains Mono',monospace;color:#64748B;font-size:0.9rem">
                Nenhum padrão detectado na amostragem atual.<br><br>
                <span style="font-size:0.75rem">Aumente o volume de dados para ampliar a detecção.</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.warning(f"{df_testa.height} padrão(ões) detectado(s)")
        st.dataframe(df_testa.to_pandas(), use_container_width=True, hide_index=True)