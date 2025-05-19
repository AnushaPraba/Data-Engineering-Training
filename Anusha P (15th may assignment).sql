-- Table creation:
create table product 
(productid int primary key auto_increment,
productname varchar(50),
category varchar(50),
price int,
stockquantity int,
supplier varchar(50));

-- Inserting values:
insert into product (productname, category, price, stockquantity, supplier) values 
('Laptop', 'Electronics', 70000, 50, 'TechMart'),
('Office Chair', 'Furniture', 5000, 100, 'HomeComfort'),
('Smartwatch', 'Electronics', 15000, 200, 'GadgetHub'),
('Desk Lamp', 'Lighting', 1200, 300, 'BrightLife'),
('Wireless Mouse', 'Electronics', 1500, 250, 'GadgetHub');

-- Crud operation:
-- 1. Add a new product: 
insert into product( productname, category, price, stockquantity, supplier) values
('Gaming Keyboard','Electronics',3500,150,'TechMart');

-- 2. Update product price: Increase the price of all Electronics products by 10%.
update product
set price = price+(price*0.1)
where category = 'Electronics';

-- 3. Delete a product: Remove the product with the ProductID = 4 (Desk Lamp).
delete from product 
where productid=4;

-- 4. Read all products: Display all products sorted by Price in descending order.
select * from product 
order by price desc;

-- Sorting and Filtering:
-- 5. Sort products by stock quantity: Display the list of products sorted by StockQuantity in ascending order.
select * from product 
order by stockquantity ;

-- 6. Filter products by category: List all products belonging to the Electronics category.
select * from product 
where category =’Electronics’;

-- 7. Filter products with AND condition: Retrieve all Electronics products priced above 5000.
select * from product 
where category =’Electronics’ and price > 5000;

-- 8. Filter products with OR condition: List all products that are either Electronics or priced below 2000.
select * from product where category =’Electronics’ or price < 2000;

-- Aggregation and Grouping:
-- 9. Calculate total stock value: Find the total stock value (Price * StockQuantity) for all products.
select productid, productname, ( price*stockquantity) as totalstockvalue
from product;

-- 10. Average price of each category: Calculate the average price of products grouped by Category.
select category,avg(price) as averageprice from product group by category;

-- 11. Total number of products by supplier: Count the total number of products supplied by GadgetHub.
select count(productid ) as totalproducts from product where supplier='GadgetHub';

-- Conditional and Pattern Matching:
-- 12. Find products with a specific keyword: Display all products whose ProductName contains the word "Wireless".
select * from product where productname like '%Wireless%';

-- 13. Search for products from multiple suppliers: Retrieve all products supplied by either TechMart or GadgetHub.
select * from product where supplier=’TechMart’ or supplier = ‘GadgetHub’;

-- 14. Filter using BETWEEN operator: List all products with a price between 1000 and 20000.
select * from product where price between 1000 and 20000;

-- Advanced Queries:
-- 15. Products with high stock: Find products where StockQuantity is greater than the average stock quantity.
select * from product
where stockquantity > (select avg(stockquantity) from product);

-- 16. Get top 3 expensive products: Display the top 3 most expensive products in the table.
select *from product
order by price desc limit 3;

-- 17. Find duplicate supplier names: Identify any duplicate supplier names in the table.
select supplier from product
group by supplier having count(*) > 1;

-- 18. Product summary: Generate a summary that shows each Category with the number of products and the total stock value.
select category, count(*) as number_of_products, sum(price * stockquantity) as total_stock_value from product group by category;

-- Join and Subqueries (if related tables are present):
-- 19. Supplier with most products: Find the supplier who provides the maximum number of products.
select supplier from product group by supplier
having count(*) = ( select max(product_count) from (
select count(*) as product_count from product
group by supplier) as counts );

-- 20. Most expensive product per category: List the most expensive product in each category.
select p.* from product p join ( select category, max(price) as max_price 
from product group by category ) as sub
on p.category = sub.category and p.price = sub.max_price;







