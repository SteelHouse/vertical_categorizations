CREATE EXTERNAL TABLE ext_tpa.ip_vertical_associations (
    ip VARCHAR(20),
    date date,
    data_source_id smallint,
    data_source_category_id int
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://mntn-data-archive-dev/vertical_categorizations/ip_vertical_associations';



ALTER TABLE ext_tpa.ip_vertical_associations
    ADD IF NOT EXISTS PARTITION(dt='2024-01-15')
    LOCATION 's3://mntn-data-archive-dev/vertical_categorizations/ip_vertical_associations/dt=2024-01-15'
