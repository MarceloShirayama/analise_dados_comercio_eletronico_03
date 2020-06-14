SELECT
    min(T1.order_purchase_timestamp) AS prim_dt_pagto,
    max(T1.order_purchase_timestamp) AS ult_dt_pagto
    
FROM `tb_orders` AS T1;