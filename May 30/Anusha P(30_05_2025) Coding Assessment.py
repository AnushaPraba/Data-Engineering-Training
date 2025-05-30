# Python Full-Spectrum Assessment (No Solutions)
# Section 1: Python Basics & Control Flow

# Q1. Write a Python program to print all odd numbers between 10 and 50.
print("Odd numbers between 10 and 50:")
print([i for i in range(10,50) if i%2!=0])

# without using list
# for i in range(10,50):
#     if i%2!=0:
#         print(i,end=' ')

# Q2. Create a function that returns whether a given year is a leap year.
def is_leap_year(year):
    return (year%400==0) or (year%4==0 and year%100!=0)

print("Is 2000 a leap year?",is_leap_year(2000))
print("Is 2004 a leap year?",is_leap_year(2004))
print("Is 2025 a leap year?",is_leap_year(2025))
print("Is 1900 a leap year?",is_leap_year(1900))

# Q3. Write a loop that counts how many times the letter 'a' appears in a given string.
string=input("Enter a string: ")
a_count=0
for i in string.lower():
    if i=='a':
        a_count+=1
print(f"The number of times the letter 'a' appears in the '{string}' is {a_count}")

#with inbuilt function
# print(f"The number of times the letter 'a' appears in the '{string}' is {string.count('a')}")

# ----------------------------------------------------------------------------------------------------

# Section 2: Collections (Lists, Tuples, Sets, Dicts)

# Q4. Create a dictionary from the following lists:
# keys = ['a', 'b', 'c']
# values = [100, 200, 300]

keys = ['a', 'b', 'c']
values = [100, 200, 300]
dic=dict(zip(keys,values))
print(dic)

#using for loop
# dic={}
# for i in range(len(keys)):
#     dic[keys[i]]=values[i]
# print(dic)

# Q5. From a list of employee salaries, extract:
# The maximum salary
# All salaries above average
# A sorted version in descending order
emp_sal=[25000,30000,37000,48000,12000,56000]
print("Maximum salary: ",max(emp_sal))
avg_sal=round(sum(emp_sal)/len(emp_sal),2)
print("Average salary: ",avg_sal)
print("Salaries above average salary: ")
for sal in emp_sal:
    if sal>avg_sal:
        print(sal)
emp_sal.sort(reverse=True)
print("Salaries sorted: ",emp_sal)

# Q6. Create a set from a list and remove duplicates. Show the difference between two
# sets:
# a = [1, 2, 3, 4]
# b = [3, 4, 5, 6]
a = [1, 2, 3, 4]
b = [3, 4, 5, 6]
a_set=set(a)
b_set=set(b)
print(f"Set difference between {a_set} and {b_set} is {a_set.difference(b_set)}")
print(f"Set difference between {b_set} and {a_set} is {b_set.difference(a_set)}")

# ------------------------------------------------------------------------------------------------

# Section 3: Functions & Classes

# Q7. Write a class Employee with __init__ , display() , and is_high_earner() methods.
# An employee is a high earner if salary > 60000.
class Employee:
    def __init__(self,emp_id,emp_name,dept,sal):
        self.emp_id=emp_id
        self.emp_name=emp_name
        self.dept=dept
        self.sal=sal
    def display(self):
        print("Employee Details:")
        print(f"Employee ID : {self.emp_id}\nName : {self.emp_name}\n"
              f"Department : {self.dept}\nSalary : {self.sal}")
    def is_high_earner(self):
        return self.sal<60000

# Q8. Create a class Project that inherits from Employee and adds project_name and hours_allocated.
class Project(Employee):
    def __init__(self,emp_id,emp_name,dept,sal,project_name,hours_allocated):
        super().__init__(self,emp_id,emp_name,dept,sal)
        self.project_name=project_name
        self.hours_allocated=hours_allocated

# Q9. Instantiate 3 employees and print whether they are high earners.
e1=Employee(101,'Kannan','Accounts',25000)
e2=Employee(102,'Mahesh','HR',70000)
e3=Employee(103,'Vishnu','IT',50000)
e1.display()
e2.display()
e3.display()
print("Is Employee 1 a high earner?",e1.is_high_earner())
print("Is Employee 2 a high earner?",e2.is_high_earner())
print("Is Employee 3 a high earner?",e3.is_high_earner())

# ---------------------------------------------------------------------------------------------

# Section 4: File Handling

# Q10. Write to a file the names of employees who belong to the 'IT' department.
import csv
with open("employees.csv",'r') as file,open('it_employees.txt','w') as file1:
    content=csv.DictReader(file)
    for row in content:
        if row['Department']=='IT':
            file1.write(row['Name']+'\n')

# Q11. Read from a text file and count the number of words.
with open('it_employees.txt','r') as file:
    words=file.read().split()
    print("No. of words in the file:",len(words))

# ------------------------------------------------------------------------------------------------

# Section 5: Exception Handling

# Q12. Write a program that accepts a number from the user and prints the square.
# Handle the case when input is not a number.
try:
    n=int(input("Enter a number: "))
    print(f"Square value of {n} is {n**2}")
except ValueError :
    print("Only integer values are expected")

# Q13. Handle a potential ZeroDivisionError in a division function.
def div(a,b):
    try:
        print(a/b)
    except ZeroDivisionError:
        print("Cannot divide by zero")
div(10,20)
div(10,0)
div(10,5)
a=int(input("Enter first number: "))
b=int(input("Enter second number: "))
div(a,b)

# ----------------------------------------------------------------------------------

# Section 6: Pandas – Reading & Exploring CSVs

# Q14. Load both employees.csv and projects.csv using Pandas.
import pandas as pd
employees=pd.read_csv("employees.csv")
projects=pd.read_csv("projects.csv")

# Q15. Display:
# First 2 rows of employees
print(employees.head(2))
# Unique values in the Department column
print("Departments:",employees['Department'].unique())
# Average salary by department
print("Average Salary by Department:\n",employees.groupby('Department')['Salary'].mean())

# Q16. Add a column TenureInYears = current year - joining year.
from datetime import datetime
current_year=datetime.now().year
employees['JoiningYear']=pd.to_datetime(employees['JoiningDate']).dt.year
employees['TenureInYears']=current_year-employees['JoiningYear']
print(employees[['Name','TenureInYears']])

# ---------------------------------------------------------------------------------------------------

# Section 7: Data Filtering, Aggregation, and Sorting

# Q17. From employees.csv , filter all IT department employees with salary > 60000.
print(employees[(employees['Department']=='IT') & (employees['Salary']>60000)])

# Q18. Group by Department and get:
# Count of employees
# Total Salary
# Average Salary
dept_grp=employees.groupby('Department')
print("Count of Employees:\n ",employees['Department'].value_counts())
print("Total salary:\n ",dept_grp['Salary'].sum())
print("Average salary:\n",dept_grp['Salary'].mean())

# Q19. Sort all employees by salary in descending order.
print(employees[['Name','Salary']].sort_values("Salary",ascending=False))

# ----------------------------------------------------------------------------------------------------------

# Section 8: Joins & Merging
# Q20. Merge employees.csv and projects.csv on EmployeeID to show project allocations.
result=pd.merge(employees,projects,on='EmployeeID')
print(result)

# Q21. List all employees who are not working on any project (left join logic).
result1=pd.merge(employees,projects,on='EmployeeID',how='left')
print("Employees who are not working on any project:")
print(result1[result1['ProjectID'].isnull()])

# Q22. Add a derived column TotalCost = HoursAllocated * (Salary / 160) in the merged dataset.
result['HourlyRate']=result['Salary']/160
result['TotalCost']=result['HoursAllocated']*result['HourlyRate']
print(result[['Name','ProjectName','TotalCost']])