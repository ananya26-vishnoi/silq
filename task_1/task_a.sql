-- List the total number of orders for each product, sorted by the number of orders:
SELECT
    p.ProductID,
    p.ProductName,
    COUNT(o.OrderID) AS NumberOfOrders
FROM
    Products p
LEFT JOIN
    Orders o ON p.ProductID = o.ProductID
GROUP BY
    p.ProductID, p.ProductName
ORDER BY
    NumberOfOrders DESC;