-- transform/dbt/models/silver/stg_propriedades.sql
-- Staging da tabela Silver: normaliza e deduplica registros do SICAR
-- Particionado por dt_referencia, clusterizado por estado

{{
  config(
    materialized = 'incremental',
    unique_key    = 'cod_car',
    partition_by  = {
      "field": "dt_referencia",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by = ['estado', 'situacao_car']
  )
}}

WITH ranked AS (
  SELECT
    cod_car,
    cpf_cnpj,
    nome_proprietario,
    municipio,
    estado,
    ROUND(area_ha, 4)                  AS area_ha,
    geometry_wkt,
    situacao_car,
    possui_embargo,
    num_embargos,
    cod_sigef,
    CAST(dt_referencia AS DATE)        AS dt_referencia,
    CAST(dt_ingestao AS TIMESTAMP)     AS dt_ingestao,
    ROW_NUMBER() OVER (
      PARTITION BY cod_car
      ORDER BY dt_ingestao DESC
    )                                  AS rn
  FROM {{ source('silver_raw', 'sicar') }}
  WHERE cod_car IS NOT NULL
    AND LENGTH(cod_car) >= 10

  {% if is_incremental() %}
    AND dt_ingestao > (SELECT MAX(dt_ingestao) FROM {{ this }})
  {% endif %}
)

SELECT
  cod_car,
  cpf_cnpj,
  nome_proprietario,
  municipio,
  estado,
  area_ha,
  geometry_wkt,
  situacao_car,
  possui_embargo,
  num_embargos,
  cod_sigef,
  dt_referencia,
  dt_ingestao
FROM ranked
WHERE rn = 1
