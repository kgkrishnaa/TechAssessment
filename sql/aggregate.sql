--Query to return sorted list of regions based on the highest average  spending

SELECT  OrderRegion , avg(OrderValue) average_spending
    FROM `plasma-ripple-261523.DataSet_Orders.Orders`
group by OrderRegion 
order by  average_spending desc 




