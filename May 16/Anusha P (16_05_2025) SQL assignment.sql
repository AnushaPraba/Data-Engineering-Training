-- table creation
Create table book ( bookid int primary key auto_increment,
title varchar(70),author varchar(70),genre varchar (50),price int,publishedyear int,stock int);

-- inserting values
insert into book (title,author,genre,price,publishedyear,stock) values
('The Alchemist','Paulo Coelho','Fiction',300,1988,50),
('Sapiens','Yuval Noah Harari','Non-Fiction',500,2011,30),
('Atomic Habits','James Clear','Self-Help',400,2018,25),
('Rich Dad Poor Dad','Robert Kiyosaki','Personal Finance',350,1997,20),
('The Lean Startup','Eric Ries','Business',450,2011,15);

-- Tasks:
-- 1. CRUD Operations:
-- 1.Add a new book: Insert a book titled "Deep Work" by Cal Newport, Genre Self-Help, Price 420, Published Year 2016, Stock 35.
Insert into book (title,author,genre,price,publishedyear,stock) values
('Deep Work','Cal Newport','Self-Help',420,2016,35);

-- 2. Update book price: Increase the price of all Self-Help books by 50.
update book
set price = price+50
where genre='Self-Help';

-- 3.Delete a book: Remove the book with BookID = 4 (Rich Dad Poor Dad).
delete from book where bookid=4;

-- 4.Read all books: Display all books sorted by Title in ascending order.
select * from book order by title asc;

-- 2. Sorting and Filtering:
-- 5.Sort by price: List books sorted by Price in descending order.
select * from book order by price desc;

-- 6.Filter by genre: Display all books belonging to the Fiction genre.
select * from book where genre='Fiction';

-- 7.Filter with AND condition: List all Self-Help books priced above 400.
select * from book where genre='Self-Help' and price > 400;

-- 8.Filter with OR condition: Retrieve all books that are either Fiction or published after 2000.
select * from book where genre='Fiction' or publishedyear>2000;

-- 3. Aggregation and Grouping:
-- 9.Total stock value:Calculate the total value of all books in stock (Price * Stock)
select bookid,title,(price*stock) as totalstockvalue from book;

-- 10.Average price by genre: Calculate the average price of books grouped by Genre.
select genre,avg(price) as averageprice from book group by genre;

-- 11.Total books by author: Count the number of books written by Paulo Coelho.
select count(*) from book where author='Paulo Coelho';

-- 4. Conditional and Pattern Matching:
-- 12.Find books with a keyword in title: List all books whose Title contains the word "The".
select * from book where title like "%The%";

-- 13.Filter by multiple conditions: Display all books by Yuval Noah Harari priced below 600.
select * from book where author='Yuval Noah Harari' and price<600;

-- 14.Find books within a price range: List books priced between 300 and 500.
select * from book where price between 300 and 500;

-- 5. Advanced Queries:
-- 15.Top 3 most expensive books: Display the top 3 books with the highest price.
select * from book order by price desc limit 3;

-- 16.Books published before a specific year: Find all books published before the year 2000.
select * from book where publishedyear<2000;

-- 17.Group by Genre: Calculate the total number of books in each Genre.
select count(*),genre from book group by genre;

-- 18.Find duplicate titles: Identify any books having the same title.
select title from book group by title having count(*) > 1 ;

-- 6. Join and Subqueries (if related tables are present):
-- 19.Author with the most books: Find the author who has written the maximum number of books.
select author from book group by author having count(*) = (select max(total) from
(select count(*)as total,author from book group by author)as counts);

-- 20.Oldest book by genre: Find the earliest published book in each genre.
select b.* from book b join (select min(publishedyear)as publishedyear,genre from book group by genre)as sub
on b.publishedyear=sub.publishedyear and b.genre=sub.genre;








