SELECT
    '{date}' AS dt_referencia,
    T1.*,
    coalesce(T2.qtde_vda, 0) AS qtde_vda_futura,
    CASE
        WHEN coalesce(T2.qtde_vda, 0) = 0
        THEN 1
        ELSE 0
        END
        AS flag_churn

FROM pre_abt_train_churn AS T1

LEFT JOIN (

    SELECT
        T2.seller_id, 
        count(DISTINCT T1.order_id) AS qtde_vda
                
    FROM tb_orders AS T1

    LEFT JOIN tb_order_items AS T2
    ON T1.order_id = T2.order_id

    WHERE T1.order_purchase_timestamp >= '{date}'
    AND T1.order_purchase_timestamp < date('{date}', '+3 month')
    -- AND T1.order_status = 'delivered'

    GROUP BY T2.seller_id

) as T2

ON T1.seller_id = T2. seller_id
