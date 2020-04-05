/* vw_ordersSubscriptionsPurchaseAndRenewals
*  Orders for subscriptions excluding trial, addons and returns with rowNumber by subscriptionID
*  The first order is the initial purchase. Subsequent orders are renewals (regardless if $0)
*  An equivalent definition is in the graphpad db, vw_ordersSubscriptionsPurchaseAndRenewal
*  This definition and the view should be kept in sync.
*  version 2020.04.02.1
*/

SELECT
	o.subscriptionid,
	o.ordernumber,
	o.invoicenumber,
	o.authcode,
	o.status,
	firstLineItem.productid,
	Row_number()
		OVER (
			partition BY subscriptionid
			ORDER BY o.ordernumber ) rowNumber
FROM   GRAPHPAD_DB.PUBLIC.orders o
			 INNER JOIN (
				-- determine the min orderDetailID for the order, can't group by productID
				SELECT Min(orderdetailid) firstOrderDetailID,
							 orderdetails.ordernumber
				 FROM   GRAPHPAD_DB.PUBLIC.orderdetails
				 GROUP  BY orderdetails.ordernumber
				) AS minOrderDetail ON minOrderDetail.ordernumber = o.ordernumber
			 -- need to join to minOrderDetail to get the productID
			 INNER JOIN GRAPHPAD_DB.PUBLIC.orderdetails firstLineItem
							 ON minOrderDetail.firstorderdetailid =
									firstLineItem.orderdetailid
WHERE  ( o.status = 'readyToPack'
					OR o.status = 'readyToShip'
					OR o.status = 'complete' ) -- valid/booked orders
			 AND o.subscriptionid IS NOT NULL --subscription orders
			 AND UPPER(o.authcode) <> 'TRIAL' -- exclude trials
			 AND UPPER(firstLineItem.productid) NOT IN ( 'ADDON' ) -- exclude addon orders
			 AND o.returntypeid IS NULL -- exclude returns
			 -- AND o.orderdate > '2020-01-01' --temp
			 --AND o.subscriptionid = 873426 --temp
			 --AND firstLineItem.productID NOT IN ('PRL1M', 'PRL1MR') -- temp
ORDER  BY subscriptionid DESC