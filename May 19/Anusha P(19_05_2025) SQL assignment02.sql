use hexa;
-- Table Schema: Product Inventory Table
-- Table Creation
create table product_inventory(product_id int auto_increment primary key,
product_name varchar(50),category varchar(50),quantity int,unit_price int,
supplier varchar(50),last_restocked date);

-- Values Insertion:
insert into product_inventory 
(product_name,category,quantity,unit_price,supplier,last_restocked) values
("Laptop","Electronics",20,70000,"TechMart",'2025-04-20'),
("Office Chair","Furniture",50,5000,"HomeComfort",'2025-04-18'),
("Smartwatch","Electronics",30,15000,"GadgetHub",'2025-04-25'),
("Desk Lamp","Lighting",80,1200,"BrightLife",'2025-04-25'),
("Wireless Mouse","Electronics",100,1500,"GadgetHub",'2025-04-30');

-- Tasks:
-- 1. CRUD Operations:
-- Add a new product: Insert a product named "Gaming Keyboard", Category Electronics, Quantity 40, UnitPrice 3500, Supplier TechMart, LastRestocked 2025-05-01.
insert into product_inventory 
(product_name,category,quantity,unit_price,supplier,last_restocked) values
("Gaming Keyboard","Electronics",40,3500,"TechMart",'2025-05-01');

select * from product_inventory;

set sql_safe_updates=0;
-- Update stock quantity: Increase the Quantity of Desk Lamp by 20.
update product_inventory set quantity=quantity+20 where product_name="Desk Lamo";
set sql_safe_updates=1;

-- Delete a discontinued product: Remove the product with ProductID = 2 (Office Chair).
delete from product_inventory where product_id=2;

-- Read all products: Display all products sorted by ProductName in ascending order.
select * from product_inventory order by product_name;

-- 2. Sorting and Filtering:
-- Sort by Quantity: List products sorted by Quantity in descending order.
select * from product_inventory order by quantity desc;

-- Filter by Category: Display all Electronics products.
select * from product_inventory where category="Electronics";

-- Filter with AND condition: List all Electronics products with Quantity > 20.
select * from product_inventory where category="Electronics" and quantity>20;

-- Filter with OR condition: Retrieve all products that belong to Electronics or have a UnitPrice below 2000.
select * from product_inventory where category="Electronics" or unit_price<2000;

-- 3. Aggregation and Grouping:
-- Total stock value calculation: Calculate the total value of all products (Quantity * UnitPrice).
select *,(quantity*unit_price) as total_value from product_inventory;

-- Average price by category: Find the average price of products grouped by Category.
select category,avg(unit_price)as average_price from product_inventory
group by category;

-- Count products by supplier: Display the number of products supplied by GadgetHub.
select sum(quantity) as total_products_by_gadgethub from product_inventory where supplier="GadgetHub";

-- 4. Conditional and Pattern Matching:
-- Find products by name prefix: List all products whose ProductName starts with 'W'.
select * from product_inventory where product_name like "W%";

-- Filter by supplier and price: Display all products supplied by GadgetHub with a UnitPrice above 10000.
select * from product_inventory where supplier="GadgetHub" and unit_price>10000;

-- Filter using BETWEEN operator: List all products with UnitPrice between 1000 and 20000.
select * from product_inventory where unit_price between 1000 and 20000;

-- 5. Advanced Queries:
-- Top 3 most expensive products: Display the top 3 products with the highest UnitPrice.
select * from product_inventory
order by unit_price desc
limit 3;

-- Products restocked recently: Find all products restocked in the last 10 days.
select * from product_inventory
where last_restocked > curdate()-interval 25 day ;

-- Group by Supplier: Calculate the total quantity of products from each Supplier.
select supplier,sum(quantity) as total_quantity 
from product_inventory group by supplier;

-- Check for low stock: List all products with Quantity less than 30.
select * from product_inventory 
where quantity<30;

-- 6. Join and Subqueries (if related tables are present):
-- Supplier with most products: Find the supplier who provides the maximum number of products.
select supplier from product_inventory group by supplier having count(*)=
(select max(total) from
(select supplier,count(*) as total from product_inventory
group by supplier)as count_table);  

-- Product with highest stock value: Find the product with the highest total stock value (Quantity * UnitPrice).
select *,(quantity*unit_price) as stock_value from product_inventory having quantity*unit_price=(
select max(stock_value) from
(select *,(quantity*unit_price) as stock_value
from product_inventory) as stock_table);

