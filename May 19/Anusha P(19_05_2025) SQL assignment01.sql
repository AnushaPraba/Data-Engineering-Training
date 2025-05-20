use hexa;
-- Table Schema: Employee Attendance Table
-- Table Creation:
create table employeeattendance (attendance_id int auto_increment primary key,employee_name varchar(50),department varchar(50),date date,status varchar(20),hours_worked int);

-- Inserting values:
 insert into employeeattendance(employee_name,department,date,status,hours_worked) values
 ("John Doe","IT","2025-05-01","Present",8),
 ("Priya Singh","HR","2025-05-01","Absent",0),
 ("Ali Khan","IT","2025-05-01","Present",7),
 ("Riya Patel","Sales","2025-05-01","Late",6),
 ("David Brown","Marketing","2025-05-01","Present",8);
 
-- Tasks:
-- 1. CRUD Operations:
-- Add a new attendance record:Insert a record for Neha Sharma, from Finance, on 2025-05-01, marked as Present, with 8 hours worked.
insert into employeeattendance(employee_name,department,date,status,hours_worked) values 
("Neha Sharma","Finance","2025-05-01","Present",8);

select * from employeeattendance;

SET SQL_SAFE_UPDATES = 0;

-- Update attendance status:Change Riya Patel's status from Late to Present.
update employeeattendance set status="Present" where employee_name="Riya Patel";

SET SQL_SAFE_UPDATES = 1;

-- Delete a record: Remove the attendance entry for Priya Singh on 2025-05-01.
delete from employeeattendance where employee_name="Priya Singh" and date="2025-05-01";

-- Read all records: Display all attendance records sorted by EmployeeName in ascending order.
select * from employeeattendance order by employee_name;

-- 2. Sorting and Filtering:
-- Sort by Hours Worked: List employees sorted by HoursWorked in descending order.
select * from employeeattendance order by hours_worked desc;

-- Filter by Department:Display all attendance records for the IT department.
select * from employeeattendance where department="IT";

-- Filter with AND condition:List all Present employees from the IT department.
select * from employeeattendance where status="Present" and department="IT";

-- Filter with OR condition: Retrieve all employees who are either Absent or Late.
select * from employeeattendance where status="Absent" or status="Late";

-- 3. Aggregation and Grouping:
-- Total Hours Worked by Department:Calculate the total hours worked grouped by Department.
select department,sum(hours_worked) as total_hours 
from employeeattendance group by department;

-- Average Hours Worked:Find the average hours worked per day across all departments.
select department,avg(hours_worked) as average_hours_worked 
from employeeattendance group by department;

-- Attendance Count by Status:Count how many employees were Present, Absent, or Late.
select status, count(*) from employeeattendance 
group by status;

-- 4. Conditional and Pattern Matching:
-- Find employees by name prefix:List all employees whose EmployeeName starts with 'R'.
select * from employeeattendance where employee_name like "R%";

-- Filter by multiple conditions:Display employees who worked more than 6 hours and are marked Present.
select * from employeeattendance where hours_worked>6 and status="Present";

-- Filter using BETWEEN operator:List employees who worked between 6 and 8 hours.
select * from employeeattendance where hours_worked between 6 and 8;

-- 5. Advanced Queries:
-- Top 2 employees with the most hours:Display the top 2 employees with the highest number of hours worked.
select * from employeeattendance 
order by hours_worked desc
limit 2;

-- Employees who worked less than the average hours:List all employees whose HoursWorked are below the average.
select * from employeeattendance where hours_worked<
(select avg(hours_worked) from employeeattendance);

-- Group by Status:Calculate the average hours worked for each attendance status (Present, Absent, Late).
select status,avg(hours_worked) as average_hours_worked
from employeeattendance group by status;

-- Find duplicate entries:Identify any employees who have multiple attendance records on the same date.
select * from employeeattendance 
where (employee_name,date) in(
select employee_name,date from employeeattendance
group by employee_name,date 
having count(*)>1);

-- 6. Join and Subqueries (if related tables are present):
-- Department with most Present employees: Find the department with the highest number of Present employees.
select department from employeeattendance
where status="Present" group by department having count(*)=
(select max(total) from 
(select department,count(*)as total from employeeattendance where status="Present"
group by department)as count_table) ;

-- Employee with maximum hours per department: Find the employee with the most hours worked in each department.
select e.* from employeeattendance e join
(select max(hours_worked) as hours_worked,department
from employeeattendance group by department)as emp
on e.department=emp.department and e.hours_worked=emp.hours_worked;

