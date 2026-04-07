-- transform/dbt/models/gold/consulta_propriedade.sql
-- Tabela Gold: visão consolidada de cada propriedade para consulta rápida
-- Usada diretamente pela API do Dados Fazenda para responder em < 2 segundos

{{
  config(
    materialized = 'table',
    cluster_by   = ['estado', 'cod_car']
  )
}}

WITH prop AS (
  SELECT * FROM {{ ref('stg_propriedades') }}
),

embargos_agg AS (
  SELECT
    cod_car,
    COUNT(*)                                          AS total_embargos,
    MAX(dt_embargo)                                   AS ultimo_embargo,
    STRING_AGG(num_tad, ', ' ORDER BY dt_embargo DESC LIMIT 3) AS tads_recentes
  FROM {{ source('silver_raw', 'embargos_ibama') }}
  GROUP BY 1
),

sigef_join AS (
  SELECT
    cod_imovel_car,
    parcela_codigo                                    AS cod_sigef_oficial,
    ROUND(area_registrada_ha, 4)                      AS area_sigef_ha,
    situacao_sigef
  FROM {{ source('silver_raw', 'sigef_incra') }}
)

SELECT
  p.cod_car,
  p.cpf_cnpj,
  p.nome_proprietario,
  p.municipio,
  p.estado,
  p.area_ha                                          AS area_car_ha,
  s.area_sigef_ha,
  ROUND(ABS(p.area_ha - COALESCE(s.area_sigef_ha, p.area_ha)), 2)
                                                     AS divergencia_area_ha,
  p.geometry_wkt,
  p.situacao_car,
  s.cod_sigef_oficial,
  s.situacao_sigef,
  COALESCE(e.total_embargos, 0) > 0                  AS possui_embargo,
  COALESCE(e.total_embargos, 0)                      AS num_embargos,
  e.ultimo_embargo,
  e.tads_recentes,
  p.dt_referencia,
  CURRENT_TIMESTAMP()                                AS dt_atualizacao

FROM prop p
LEFT JOIN sigef_join  s ON s.cod_imovel_car = p.cod_car
LEFT JOIN embargos_agg e ON e.cod_car       = p.cod_car
