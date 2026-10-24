-- Sample Silver Table for ML Task
CREATE OR REFRESH MATERIALIZED VIEW silver_sample_ml
AS
SELECT 
    *
FROM health_silver
LIMIT 2;
