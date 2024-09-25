-- Rolling average sql 
-- This is not a true rolling average and needs more work

SELECT * , round ( avg(OrderValue)
OVER (
        PARTITION BY customerid
        ORDER BY orderdate        
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) , 1)  AS rolling_avg_30_days 
    FROM `plasma-ripple-261523.DataSet_Orders.Orders`
order by CustomerId, orderdate
