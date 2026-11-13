#!/usr/bin/env python3
"""
Script de Criação Automatizada do Dashboard Executivo no Apache Superset
Conecta às tabelas Gold Marts:
- lakehouse_main.mart_mobilidade_diaria_zonas
- lakehouse_main.fato_corridas
Configura Slices nativos com query_context completo e layout responsivo de 12 colunas,
seguindo a especificação canônica do Apache Superset v2 (compatível com React ErrorBoundary).
"""
import json
from superset.app import create_app

app = create_app()

with app.app_context():
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    from superset.connectors.sqla.models import SqlaTable
    from superset import db

    t_mart = db.session.query(SqlaTable).filter_by(table_name="mart_mobilidade_diaria_zonas").first()
    t_fato = db.session.query(SqlaTable).filter_by(table_name="fato_corridas").first()

    if not t_mart or not t_fato:
        print("Erro: Tabelas mart_mobilidade_diaria_zonas ou fato_corridas não encontradas no Superset!")
        exit(1)

    # 1. Limpa dashboard e slices antigos
    existing_dash = db.session.query(Dashboard).filter_by(dashboard_title="Painel Executivo: Mobilidade Urbana NYC TLC").first()
    if existing_dash:
        print(f"Removendo dashboard antigo (ID {existing_dash.id})...")
        for s in list(existing_dash.slices):
            db.session.delete(s)
        db.session.delete(existing_dash)
        db.session.commit()

    # Também remove slices orfãos com esses nomes
    nomes_slices = [
        "Total de Corridas", "Faturamento Bruto", "Velocidade Média da Frota",
        "Passageiros Transportados", "Demanda por Turno do Dia",
        "Faturamento por Distrito (Borough)", "Perfil de Rotas Urbanas",
        "Top Bairros/Zonas por Demanda e Faturamento"
    ]
    for s in db.session.query(Slice).filter(Slice.slice_name.in_(nomes_slices)).all():
        db.session.delete(s)
    db.session.commit()

    slices = []

    # KPI 1: Total de Corridas
    m1 = {"expressionType": "SQL", "sqlExpression": "SUM(qtd_corridas)", "label": "Total de Corridas"}
    p1 = {
        "datasource": f"{t_mart.id}__table",
        "viz_type": "big_number_total",
        "metric": m1,
        "subheader": "Viagens auditadas pela Alfândega",
        "header_font_size": 0.35,
        "subheader_font_size": 0.15,
        "y_axis_format": ",d",
        "time_range": "No filter",
        "adhoc_filters": [],
        "url_params": {},
        "queryFields": {"metric": "metrics"},
    }
    qc1 = {
        "datasource": {"id": t_mart.id, "type": "table"},
        "queries": [{"metrics": [m1], "filters": [], "row_limit": 1000}],
        "form_data": p1, "result_format": "json", "result_type": "full"
    }
    s1 = Slice(slice_name="Total de Corridas", viz_type="big_number_total", datasource_type="table", datasource_id=t_mart.id, params=json.dumps(p1), query_context=json.dumps(qc1))
    slices.append(s1)

    # KPI 2: Faturamento Total (USD)
    m2 = {"expressionType": "SQL", "sqlExpression": "SUM(faturamento_bruto_usd)", "label": "Faturamento (USD)"}
    p2 = {
        "datasource": f"{t_mart.id}__table",
        "viz_type": "big_number_total",
        "metric": m2,
        "subheader": "Receita bruta faturada",
        "header_font_size": 0.35,
        "subheader_font_size": 0.15,
        "y_axis_format": "$,.2f",
        "time_range": "No filter",
        "adhoc_filters": [],
        "url_params": {},
        "queryFields": {"metric": "metrics"},
    }
    qc2 = {
        "datasource": {"id": t_mart.id, "type": "table"},
        "queries": [{"metrics": [m2], "filters": [], "row_limit": 1000}],
        "form_data": p2, "result_format": "json", "result_type": "full"
    }
    s2 = Slice(slice_name="Faturamento Bruto", viz_type="big_number_total", datasource_type="table", datasource_id=t_mart.id, params=json.dumps(p2), query_context=json.dumps(qc2))
    slices.append(s2)

    # KPI 3: Velocidade Média do Trânsito (km/h)
    m3 = {"expressionType": "SQL", "sqlExpression": "ROUND(AVG(velocidade_media_kmh), 1)", "label": "Velocidade Média (km/h)"}
    p3 = {
        "datasource": f"{t_mart.id}__table",
        "viz_type": "big_number_total",
        "metric": m3,
        "subheader": "Índice de fluidez viária urbana",
        "header_font_size": 0.35,
        "subheader_font_size": 0.15,
        "y_axis_format": ".1f",
        "time_range": "No filter",
        "adhoc_filters": [],
        "url_params": {},
        "queryFields": {"metric": "metrics"},
    }
    qc3 = {
        "datasource": {"id": t_mart.id, "type": "table"},
        "queries": [{"metrics": [m3], "filters": [], "row_limit": 1000}],
        "form_data": p3, "result_format": "json", "result_type": "full"
    }
    s3 = Slice(slice_name="Velocidade Média da Frota", viz_type="big_number_total", datasource_type="table", datasource_id=t_mart.id, params=json.dumps(p3), query_context=json.dumps(qc3))
    slices.append(s3)

    # KPI 4: Total de Passageiros
    m4 = {"expressionType": "SQL", "sqlExpression": "SUM(total_passageiros)", "label": "Passageiros"}
    p4 = {
        "datasource": f"{t_mart.id}__table",
        "viz_type": "big_number_total",
        "metric": m4,
        "subheader": "Pessoas atendidas no período",
        "header_font_size": 0.35,
        "subheader_font_size": 0.15,
        "y_axis_format": ",d",
        "time_range": "No filter",
        "adhoc_filters": [],
        "url_params": {},
        "queryFields": {"metric": "metrics"},
    }
    qc4 = {
        "datasource": {"id": t_mart.id, "type": "table"},
        "queries": [{"metrics": [m4], "filters": [], "row_limit": 1000}],
        "form_data": p4, "result_format": "json", "result_type": "full"
    }
    s4 = Slice(slice_name="Passageiros Transportados", viz_type="big_number_total", datasource_type="table", datasource_id=t_mart.id, params=json.dumps(p4), query_context=json.dumps(qc4))
    slices.append(s4)

    # Gráfico 5: Corridas por Turno do Dia
    m5 = {"expressionType": "SQL", "sqlExpression": "SUM(qtd_corridas)", "label": "Corridas"}
    p5 = {
        "datasource": f"{t_mart.id}__table",
        "viz_type": "pie",
        "groupby": ["turno_viagem"],
        "metric": m5,
        "donut": True,
        "innerRadius": 40,
        "outerRadius": 70,
        "show_labels": True,
        "labels_outside": True,
        "label_line": True,
        "label_type": "key_percent",
        "color_scheme": "supersetColors",
        "time_range": "No filter",
        "adhoc_filters": [],
        "url_params": {},
        "queryFields": {"groupby": "groupby", "metric": "metrics"},
        "row_limit": 100,
        "show_legend": True,
    }
    qc5 = {
        "datasource": {"id": t_mart.id, "type": "table"},
        "queries": [{
            "columns": ["turno_viagem"],
            "metrics": [m5],
            "orderby": [[m5, False]],
            "filters": [],
            "row_limit": 100
        }],
        "form_data": p5, "result_format": "json", "result_type": "full"
    }
    s5 = Slice(slice_name="Demanda por Turno do Dia", viz_type="pie", datasource_type="table", datasource_id=t_mart.id, params=json.dumps(p5), query_context=json.dumps(qc5))
    slices.append(s5)

    # Gráfico 6: Faturamento por Distrito (Borough)
    m6 = {"expressionType": "SQL", "sqlExpression": "SUM(faturamento_bruto_usd)", "label": "Receita (USD)"}
    p6 = {
        "datasource": f"{t_mart.id}__table",
        "viz_type": "pie",
        "groupby": ["distrito_embarque"],
        "metric": m6,
        "donut": False,
        "innerRadius": 0,
        "outerRadius": 70,
        "show_labels": True,
        "labels_outside": True,
        "label_line": True,
        "label_type": "key_value",
        "number_format": "$,.2f",
        "color_scheme": "d3Category20",
        "time_range": "No filter",
        "adhoc_filters": [],
        "url_params": {},
        "queryFields": {"groupby": "groupby", "metric": "metrics"},
        "row_limit": 100,
        "show_legend": True,
    }
    qc6 = {
        "datasource": {"id": t_mart.id, "type": "table"},
        "queries": [{
            "columns": ["distrito_embarque"],
            "metrics": [m6],
            "orderby": [[m6, False]],
            "filters": [],
            "row_limit": 100
        }],
        "form_data": p6, "result_format": "json", "result_type": "full"
    }
    s6 = Slice(slice_name="Faturamento por Distrito (Borough)", viz_type="pie", datasource_type="table", datasource_id=t_mart.id, params=json.dumps(p6), query_context=json.dumps(qc6))
    slices.append(s6)

    # Gráfico 7: Distribuição de Rotas (Intra vs Inter vs Aeroportos)
    m7 = {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Viagens"}
    p7 = {
        "datasource": f"{t_fato.id}__table",
        "viz_type": "pie",
        "groupby": ["tipo_rota"],
        "metric": m7,
        "donut": True,
        "innerRadius": 40,
        "outerRadius": 70,
        "show_labels": True,
        "labels_outside": True,
        "label_line": True,
        "label_type": "key_percent",
        "color_scheme": "googleCategory20c",
        "time_range": "No filter",
        "adhoc_filters": [],
        "url_params": {},
        "queryFields": {"groupby": "groupby", "metric": "metrics"},
        "row_limit": 100,
        "show_legend": True,
    }
    qc7 = {
        "datasource": {"id": t_fato.id, "type": "table"},
        "queries": [{
            "columns": ["tipo_rota"],
            "metrics": [m7],
            "orderby": [[m7, False]],
            "filters": [],
            "row_limit": 100
        }],
        "form_data": p7, "result_format": "json", "result_type": "full"
    }
    s7 = Slice(slice_name="Perfil de Rotas Urbanas", viz_type="pie", datasource_type="table", datasource_id=t_fato.id, params=json.dumps(p7), query_context=json.dumps(qc7))
    slices.append(s7)

    # Gráfico 8: Tabela Top Zonas por Faturamento e Velocidade
    m8_1 = {"expressionType": "SQL", "sqlExpression": "SUM(qtd_corridas)", "label": "Total Corridas"}
    m8_2 = {"expressionType": "SQL", "sqlExpression": "SUM(faturamento_bruto_usd)", "label": "Faturamento (USD)"}
    m8_3 = {"expressionType": "SQL", "sqlExpression": "ROUND(AVG(ticket_medio_usd), 2)", "label": "Ticket Médio (USD)"}
    m8_4 = {"expressionType": "SQL", "sqlExpression": "ROUND(AVG(velocidade_media_kmh), 1)", "label": "Velocidade Média (km/h)"}
    m8_5 = {"expressionType": "SQL", "sqlExpression": "SUM(total_gorjetas_usd)", "label": "Total Gorjetas (USD)"}
    p8 = {
        "datasource": f"{t_mart.id}__table",
        "viz_type": "table",
        "groupby": ["distrito_embarque", "nome_zona_embarque"],
        "metrics": [m8_1, m8_2, m8_3, m8_4, m8_5],
        "all_columns": [],
        "percent_metrics": [],
        "order_by_cols": [],
        "order_desc": True,
        "query_mode": "aggregate",
        "row_limit": 20,
        "page_length": 10,
        "show_cell_bars": True,
        "table_timestamp_format": "smart_date",
        "time_range": "No filter",
        "adhoc_filters": [],
        "url_params": {},
        "queryFields": {"groupby": "groupby", "metrics": "metrics"},
    }
    qc8 = {
        "datasource": {"id": t_mart.id, "type": "table"},
        "queries": [{
            "columns": ["distrito_embarque", "nome_zona_embarque"],
            "metrics": [m8_1, m8_2, m8_3, m8_4, m8_5],
            "orderby": [[m8_1, False]],
            "filters": [],
            "row_limit": 20
        }],
        "form_data": p8, "result_format": "json", "result_type": "full"
    }
    s8 = Slice(slice_name="Top Bairros/Zonas por Demanda e Faturamento", viz_type="table", datasource_type="table", datasource_id=t_mart.id, params=json.dumps(p8), query_context=json.dumps(qc8))
    slices.append(s8)

    # Salva os slices primeiro para obter os IDs e UUIDs
    for s in slices:
        db.session.add(s)
    db.session.commit()

    # Atualiza o slice_id nos params de cada slice
    for s in slices:
        p = json.loads(s.params)
        p["slice_id"] = s.id
        s.params = json.dumps(p)
    db.session.commit()

    grid_layout = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {
            "type": "ROOT",
            "id": "ROOT_ID",
            "children": ["GRID_ID"]
        },
        "GRID_ID": {
            "type": "GRID",
            "id": "GRID_ID",
            "children": ["ROW_KPIS", "ROW_CHARTS_1", "ROW_CHARTS_2", "ROW_TABLE"],
            "parents": ["ROOT_ID"]
        },
        "HEADER_ID": {
            "type": "HEADER",
            "id": "HEADER_ID",
            "meta": {
                "text": "Painel Executivo: Mobilidade Urbana NYC TLC"
            }
        },
        "ROW_KPIS": {
            "type": "ROW",
            "id": "ROW_KPIS",
            "children": [f"CHART-{s1.id}", f"CHART-{s2.id}", f"CHART-{s3.id}", f"CHART-{s4.id}"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"0": "ROOT_ID", "background": "BACKGROUND_TRANSPARENT"}
        },
        f"CHART-{s1.id}": {
            "type": "CHART",
            "id": f"CHART-{s1.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_KPIS"],
            "meta": {
                "width": 3,
                "height": 30,
                "chartId": s1.id,
                "sliceName": s1.slice_name,
                "uuid": str(s1.uuid)
            }
        },
        f"CHART-{s2.id}": {
            "type": "CHART",
            "id": f"CHART-{s2.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_KPIS"],
            "meta": {
                "width": 3,
                "height": 30,
                "chartId": s2.id,
                "sliceName": s2.slice_name,
                "uuid": str(s2.uuid)
            }
        },
        f"CHART-{s3.id}": {
            "type": "CHART",
            "id": f"CHART-{s3.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_KPIS"],
            "meta": {
                "width": 3,
                "height": 30,
                "chartId": s3.id,
                "sliceName": s3.slice_name,
                "uuid": str(s3.uuid)
            }
        },
        f"CHART-{s4.id}": {
            "type": "CHART",
            "id": f"CHART-{s4.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_KPIS"],
            "meta": {
                "width": 3,
                "height": 30,
                "chartId": s4.id,
                "sliceName": s4.slice_name,
                "uuid": str(s4.uuid)
            }
        },
        "ROW_CHARTS_1": {
            "type": "ROW",
            "id": "ROW_CHARTS_1",
            "children": [f"CHART-{s5.id}", f"CHART-{s6.id}"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"0": "ROOT_ID", "background": "BACKGROUND_TRANSPARENT"}
        },
        f"CHART-{s5.id}": {
            "type": "CHART",
            "id": f"CHART-{s5.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_CHARTS_1"],
            "meta": {
                "width": 6,
                "height": 55,
                "chartId": s5.id,
                "sliceName": s5.slice_name,
                "uuid": str(s5.uuid)
            }
        },
        f"CHART-{s6.id}": {
            "type": "CHART",
            "id": f"CHART-{s6.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_CHARTS_1"],
            "meta": {
                "width": 6,
                "height": 55,
                "chartId": s6.id,
                "sliceName": s6.slice_name,
                "uuid": str(s6.uuid)
            }
        },
        "ROW_CHARTS_2": {
            "type": "ROW",
            "id": "ROW_CHARTS_2",
            "children": [f"CHART-{s7.id}"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"0": "ROOT_ID", "background": "BACKGROUND_TRANSPARENT"}
        },
        f"CHART-{s7.id}": {
            "type": "CHART",
            "id": f"CHART-{s7.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_CHARTS_2"],
            "meta": {
                "width": 12,
                "height": 55,
                "chartId": s7.id,
                "sliceName": s7.slice_name,
                "uuid": str(s7.uuid)
            }
        },
        "ROW_TABLE": {
            "type": "ROW",
            "id": "ROW_TABLE",
            "children": [f"CHART-{s8.id}"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"0": "ROOT_ID", "background": "BACKGROUND_TRANSPARENT"}
        },
        f"CHART-{s8.id}": {
            "type": "CHART",
            "id": f"CHART-{s8.id}",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_TABLE"],
            "meta": {
                "width": 12,
                "height": 65,
                "chartId": s8.id,
                "sliceName": s8.slice_name,
                "uuid": str(s8.uuid)
            }
        },
    }

    dash = Dashboard(
        dashboard_title="Painel Executivo: Mobilidade Urbana NYC TLC",
        slug="mobilidade-urbana-nyc",
        published=True,
        slices=slices,
        position_json=json.dumps(grid_layout),
        json_metadata=json.dumps({
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "refresh_frequency": 0,
            "default_filters": "{}",
            "color_scheme": "supersetColors",
            "label_colors": {}
        })
    )
    db.session.add(dash)
    db.session.commit()

    print(f"✅ Dashboard criado com sucesso! ID: {dash.id} | Slug: {dash.slug} | Total de gráficos: {len(slices)}")
