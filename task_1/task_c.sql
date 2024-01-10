-- Display users who have not placed any orders:
SELECT
    u.UserID,
    u.SignUpDate,
    u.Location
FROM
    Users u
LEFT JOIN
    Orders o ON u.UserID = o.UserID
WHERE
    o.OrderID IS NULL;