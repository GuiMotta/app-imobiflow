# ImobiFlow — App Streamlit

## O que é
Dashboard interno para Viviane Tolentino: análise de mercado, captação de imóveis e geração de pitch IA + PDF para clientes.
URL: https://imobiflow.streamlit.app

## Fonte de dados
Supabase PostgreSQL → tabela `public.imoveis` (alimentada pelo pipeline Databricks)

## Abas / Páginas
- **Mercado** — gráficos e análises do mercado imobiliário do DF
- **Apresentação p/ Cliente** — filtros da barra lateral como perfil do cliente, busca, R$/m² vs. média do bairro e botão "Montar apresentação" → pitch IA + PDF (`?imovel=CODIGO`) e captação (`?captar=CODIGO`)
- **Oportunidades** — captação de imóveis para prospecção

## Páginas de detalhe (query params)
- `?imovel=CODIGO` → Pitch IA (Groq) + seleção de fotos + geração de PDF
- `?captar=CODIGO` → Fotos do anúncio + Google Street View + dados do imóvel
- `?pitch=CODIGO`  → Pitch IA direto

## Autenticação
- Login simples via `st.secrets["APP_PASSWORD"]`
- Token de sessão para links entre abas

## Integrações
- **Groq API**: modelo `llama-3.3-70b-versatile` (free tier) — gera pitch para WhatsApp
- **Supabase**: host `aws-1-sa-east-1.pooler.supabase.com:6543`

## PDF gerado
- Fonte Montserrat
- Fotos selecionáveis (até 15, padrão primeiras 6)
- Fechamento: "Abraço, Viviane!" (customizável)
- Tom profissional, ZERO emojis, sem condomínio/IPTU (dados incompletos)
- Não expor link do DF Imóveis nos materiais

## Secrets (Streamlit Cloud)
- `DATABASE_URL`
- `APP_PASSWORD`
- `GROQ_API_KEY`

## Padrões
- Valores no formato BR: R$ 1.099.758
- Arquivo principal: `app.py`
