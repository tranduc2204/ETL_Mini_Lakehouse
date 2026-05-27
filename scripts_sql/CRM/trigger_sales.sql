CREATE TABLE cdc_log (
    log_id SERIAL PRIMARY KEY,
    operation_type VARCHAR(10),
    table_name VARCHAR(30),
    id INT,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION track_sales_crm_changes()
RETURNS TRIGGER AS $$
BEGIN

    -- INSERT
    IF TG_OP = 'INSERT' THEN

        INSERT INTO cdc_log (
            operation_type,
            id,
			table_name,
            changed_at
        )
        VALUES (
            'INSERT',
            NEW.sls_ord_num,
			'crm_sales',
            CURRENT_TIMESTAMP
        );

        RETURN NEW;
    END IF;

    -- UPDATE
    IF TG_OP = 'UPDATE' THEN

        INSERT INTO cdc_log (
            operation_type,
            id,
			table_name,
            changed_at
        )
        VALUES (
            'UPDATE',
            NEW.sls_ord_num,
			'crm_sales',
            CURRENT_TIMESTAMP
        );

        -- auto update updated_at
        NEW.updated_at = CURRENT_TIMESTAMP;

        RETURN NEW;
    END IF;

    -- DELETE
    IF TG_OP = 'DELETE' THEN

        INSERT INTO cdc_log (
            operation_type,
            id,
			table_name,
            changed_at
        )
        VALUES (
            'DELETE',
            OLD.sls_ord_num,
			'crm_sales',
            CURRENT_TIMESTAMP
        );

        RETURN OLD;
    END IF;

    RETURN NULL;

END;
$$ LANGUAGE plpgsql;



create or replace TRIGGER track_sales_crm_changes
BEFORE INSERT OR UPDATE OR DELETE
ON sales_details
FOR EACH ROW
EXECUTE FUNCTION track_sales_crm_changes();








--------- test hoạt động
SELECT *
FROM prd_info t 



INSERT INTO cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date,
    updated_at
)
VALUES (
    1,
    'CUST001',
    'Dustin',
    'Tran',
    'Single',
    'Male',
    '2026-05-23',
    NOW()
);

update cust_info ci  
set cst_firstname = 'test'
where cst_key = 'CUST001'

delete  from cust_info 
where cst_key = 'CUST001'


select *
from cust_info
where cst_key = 'CUST001'


select *
from cdc_log





