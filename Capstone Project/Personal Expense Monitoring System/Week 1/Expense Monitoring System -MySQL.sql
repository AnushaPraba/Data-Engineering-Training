create database expense_monitor;

use expense_monitor;

-- Creating user table
create table users (
    user_id int auto_increment primary key,
    username varchar(50) not null unique,
    email varchar(100),
    created_at timestamp default current_timestamp
);

-- Creating categories table
create table categories (
    category_id int auto_increment primary key,
    category_name varchar(50) not null unique
);

-- Creating expenses table
create table expenses (
    expense_id int auto_increment primary key,
    user_id int not null,
    category_id int not null,
    amount decimal(10, 2) not null,
    expense_date date not null,
    description varchar(255),
    foreign key (user_id) references users(user_id),
    foreign key (category_id) references categories(category_id)
);

-- Inserting sample values into users table
insert into users (username, email) values ('aswin_kumar', 'aswink@gmail.com');
insert into users (username, email) values ('arun_prasath', 'arun95@gmail.com');

-- Inserting sample values into categories table
insert into categories (category_name) 
values
('Household Essentials'),
('Utilities & Bills'),
('Transport'),
('Food'),
('Healthcare'),
('Education'),
('Shopping'),
('Entertainment'),
('Savings & Investments'),
('Miscellaneous');

-- Inserting sample values into expenses table
insert into expenses (user_id, category_id, amount, expense_date, description) values
(1, 1, 3200.00, '2025-05-01', 'Groceries from DMart'),
(1, 4, 850.00,  '2025-05-03', 'Dinner at Saravana Bhavan'),
(2, 3, 1000.00, '2025-05-02', 'Uber ride to airport'),
(2, 5, 450.00,  '2025-05-04', 'Doctor visit and medicine'),
(2, 9, 3000.00, '2025-05-05', 'Monthly SIP in Axis Mutual Fund');

-- CRUD operations:
-- Create/Insert - done

-- Read/Retreive
select * from expenses;

-- Update
update expenses set amount = 1300.00, description = 'Updated BSNL broadband bill'
where expense_id = 1;

-- Delete
delete from expenses
where expense_id = 1;

-- Stored Procedure
DELIMITER //
CREATE PROCEDURE GetMonthlyExpenseSummary(IN uid INT, IN exp_month VARCHAR(7))
BEGIN
    SELECT 
        c.category_name,
        SUM(e.amount) AS total_amount
    FROM expenses e
    JOIN categories c ON e.category_id = c.category_id
    WHERE e.user_id = uid
      AND DATE_FORMAT(e.expense_date, '%Y-%m') = exp_month
    GROUP BY c.category_name;
END //
DELIMITER ;

call GetMonthlyExpenseSummary(1, '2025-05');
