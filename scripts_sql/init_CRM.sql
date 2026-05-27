-- bảng lưu lần cuối cùng update 
CREATE TABLE crm_pipeline_state (
    pipeline_name VARCHAR PRIMARY KEY,
    last_watermark TIMESTAMP
);

-- TABLE ORDER_CDC_LOG 
-- lưu thong tin insert update delete của tbale 
CREATE TABLE cdc_log (
    log_id SERIAL PRIMARY KEY,
    operation_type VARCHAR(10),
    table_name VARCHAR(30),
    id INT,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- table  lưu những bronze đgộpã 
CREATE TABLE processed_files (
    file_name TEXT PRIMARY KEY,
    processed_at TIMESTAMP
    
);
















CREATE OR REPLACE FUNCTION track_crm_cust_changes()
RETURNS TRIGGER AS $$
BEGIN

    -- INSERT
    IF TG_OP = 'INSERT' THEN

        INSERT INTO orders_cdc_log (
            operation_type,
            id,
			table_name,
            changed_at
        )
        VALUES (
            'INSERT',
            NEW.id,
			'cust',
            CURRENT_TIMESTAMP
        );

        RETURN NEW;
    END IF;

    -- UPDATE
    IF TG_OP = 'UPDATE' THEN

        INSERT INTO orders_cdc_log (
            operation_type,
            id,
			table_name,
            changed_at
        )
        VALUES (
            'UPDATE',
            NEW.id,
			'cust',
            CURRENT_TIMESTAMP
        );

        -- auto update updated_at
        NEW.updated_at = CURRENT_TIMESTAMP;

        RETURN NEW;
    END IF;

    -- DELETE
    IF TG_OP = 'DELETE' THEN

        INSERT INTO orders_cdc_log (
            operation_type,
            id,
			table_name,
            changed_at
        )
        VALUES (
            'DELETE',
            OLD.id,
			'cust',
            CURRENT_TIMESTAMP
        );

        RETURN OLD;
    END IF;

    RETURN NULL;

END;
$$ LANGUAGE plpgsql;



CREATE TRIGGER crm_cust_cdc_trigger
BEFORE INSERT OR UPDATE OR DELETE
ON cust_info
FOR EACH ROW
EXECUTE FUNCTION track_crm_cust_changes();
