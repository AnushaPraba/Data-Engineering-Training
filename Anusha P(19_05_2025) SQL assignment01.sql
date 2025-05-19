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


