SELECT DISTINCT id AS idRegistro,
       nome_bairro AS nomeBairro,
       CAST(latitude AS DOUBLE) AS latitude,
       CAST(longitude AS DOUBLE) AS longitude

FROM bronze.climapetropolis.registros