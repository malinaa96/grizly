SELECT
ORDER AS Bookings,
         Part,
         Customer,
         Value
FROM Orders
ORDER BY Bookings DESC,
         Part ASC,
         Value ASC