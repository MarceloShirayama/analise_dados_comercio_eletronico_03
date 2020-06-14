DROP TABLE IF EXISTS pre_abt_{stage}_churn;
CREATE TABLE pre_abt_{stage}_churn AS 

    SELECT
        T1.seller_id,
        max(T1.dt_venda) AS dt_ult_vda,
        
        /* Totais */
        sum(T1.price) AS receita_total,
        count(DISTINCT T1.order_id) AS qtde_vdas,
        count(T1.product_id) AS qtde_itens_total,
        count(DISTINCT T1.product_id) AS qtde_itens_dist_total,
        sum(T1.freight_value) AS frete_total,

        /* Médias por pedido (venda)*/
        sum(T1.price) / count(DISTINCT T1.order_id) AS receita_por_venda,
        count(T1.product_id) / count(DISTINCT T1.order_id) AS items_por_vda,
        sum(T1.freight_value) / count(DISTINCT T1.product_id) AS frete_por_vda,
        sum(T1.freight_value) / count(DISTINCT T1.order_id) AS frete_por_item,

        count(DISTINCT strftime("%m", dt_venda)) /6. AS proporcao_ativacao

    FROM(

        SELECT
            T1.order_id, 
            T1.order_purchase_timestamp AS dt_venda, 
            CASE
                WHEN 
                    T1.order_delivered_customer_date > 
                    T1.order_estimated_delivery_date
                THEN 1
                ELSE 0
                END
                AS flag_atraso,
            T2.seller_id,
            T2.product_id,
            T2.price,
            T2.freight_value,
            T3.seller_state,
            T4.product_category_name
                
        FROM tb_orders AS T1

        LEFT JOIN tb_order_items AS T2
        ON T1.order_id = T2.order_id

        LEFT JOIN tb_sellers AS T3
        ON T2.seller_id = T3.seller_id

        LEFT JOIN tb_products AS T4
        ON T2.product_id = T4.product_id

        WHERE T1.order_purchase_timestamp < '{date}'
        AND T1.order_purchase_timestamp >= date('{date}', '-6 month')
        AND T1.order_status = 'delivered'

    ) AS T1

    GROUP BY T1.seller_id

    HAVING  max(T1.dt_venda) >=  date('{date}', '-3 month')

;