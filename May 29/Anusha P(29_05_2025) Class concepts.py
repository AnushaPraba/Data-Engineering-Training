# FUNCTIONS (Exercises 1–3)
# 1. Prime Number Checker
# Write a function is_prime(n) that returns True if n is prime, else False.
# Use it to print all prime numbers between 1 and 100.
def is_prime(n):
    for i in range(2,int(n**0.5)+1):
        if n%i==0:
            return False
    return True

print("Prime Numbers between 1 and 100")
for i in range(2,100):
    res=is_prime(i)
    if res:
        print(i)

# 2. Temperature Converter
# Write a function convert_temp(value, unit) that converts:
# Celsius to Fahrenheit
# Fahrenheit to Celsius
# Use conditionals inside the function.
def convert_temp(val,unit):
    if unit=="C":
        temp=(val*(9/5)) + 32
    if unit=="F":
        temp=(val-32)*(5/9)
    return temp

print("---Temperature converter---")
print("1.Celsius to Fahrenheit")
print("2.Fahrenheit to Celsius")
ch=input("Enter your choice: ")
if ch=='1':
    val=float(input("Enter the temperature value(in Celsius):"))
    print("Celsius to Fahrenheit:",convert_temp(val,"C"))
elif ch=='2':
    val = float(input("Enter the temperature value(in Fahrenheit):"))
    print("Fahrenheit to Celsius:", convert_temp(val, "F"))
else:
    print("Invalid Choice")

# 3. Recursive Factorial Function
# Create a function factorial(n) using recursion to return the factorial of a number.
def factorial(n):
    if n<=1:
        return 1
    return n*factorial(n-1)
n=int(input("Enter a number"))
print(f"Factorial of {n} is",factorial(n))

# ---------------------------------------------------------------------------------------------

# CLASSES (Exercises 4–7)
# 4. Class: Rectangle
# Attributes: length , width
# Methods:
# area()
# perimeter()
# is_square() → returns True if length == width

class Rectangle:
    def __init__(self,length,breadth):
        self.length=length
        self.breadth=breadth
    def area(self):
        return self.length*self.breadth
    def perimeter(self):
        return 2*(self.length+self.breadth)
    def is_square(self):
        if self.length==self.breadth:
            return True
        else:
            return False

r1=Rectangle(4,6)
print("Area of the Rectangle: ",r1.area())
print("Perimeter of the Rectangle: ",r1.perimeter())
print("Is that a square: ",r1.is_square())

# 5. Class: BankAccount
# Attributes: name , balance
# Methods:
# deposit(amount)
# withdraw(amount)
# get_balance()
# Prevent withdrawal if balance is insufficient

class BankAccount:
    def __init__(self,name,balance):
        self.name=name
        self.balance=balance
    def deposit(self,amount):
        if amount<0:
            print("Amount value cannot be negative")
            return
        self.balance+=amount
        print("Amount deposited successfully")
        print("Current balance:", self.balance)
        return
    def withdraw(self,amount):
        if amount<0:
            print("Amount value cannot be negative")
            return
        if amount>self.balance:
            print("Balance is insufficient")
        else:
            self.balance-=amount
            print("Amount withdrawn successfully")
            print("Current balance:",self.balance)
        return
    def get_balance(self):
        return self.balance

acc1=BankAccount("Keerthi",50000.00)
acc1.deposit(2000)
acc1.withdraw(53000)
print("Current balance: ", acc1.get_balance())

# 6. Class: Book
# Attributes: title , author , price , in_stock
# Method: sell(quantity)
# Reduces stock
# Throws an error if quantity exceeds stock
class Book:
    def __init__(self,title,author,price,in_stock):
        self.title=title
        self.author=author
        self.price=price
        self.in_stock=in_stock
    def sell(self,quantity):
        if quantity>self.in_stock:
            print(f"Only {self.in_stock} books are available.")
        else:
            self.in_stock-=quantity
            print(f"{quantity} books sold")

b1=Book('One life','Vignesh Kae',2000,15)
b2=Book('Right Direction','Zayn Malik',12000,5)
b2.sell(10)
b1.sell(5)

# 7. Student Grade System
# Create a class Student with:
# Attributes: name , marks (a list)
# Method:
# average()
# grade() — returns A/B/C/F based on average

class Student:
    def __init__(self,name,marks):
        self.name=name
        self.marks=marks
    def average(self):
        return round(sum(self.marks)/len(self.marks),2)
    def grade(self):
        avg=self.average()
        if avg>=90:
            return "A"
        elif avg>=80:
            return "B"
        elif avg>=70:
            return "C"
        else:
            return "F"

s1=Student("Anush",[90,98,85,68,76])
print(f"Percentage of {s1.name} : {s1.average()}")
print(f"Grade of {s1.name}: {s1.grade()}")

# ------------------------------------------------------------------------------------------

# INHERITANCE (Exercises 8–10)
# 8. Person → Employee
# Class Person : name , age
# Class Employee inherits Person , adds emp_id , salary
# Method display_info() shows all details
class Person:
    def __init__(self,name,age):
        self.name=name
        self.age=age
    def display_info(self):
        print(f"Name:{self.name}")
        print(f"Age:{self.age}")

class Employee(Person):
    def __init__(self,name,age,emp_id,salary):
        super().__init__(name,age)
        self.emp_id=emp_id
        self.salary=salary
    def display_info(self):
        super().display_info()
        print(f"Employee ID:{self.emp_id}")
        print(f"Salary:{self.salary}")

e1=Employee("Karan",35,101,75000)
e1.display_info()

# 9. Vehicle → Car , Bike
# Base Class: Vehicle(name, wheels)
# Subclasses:
# Car : additional attribute fuel_type
# Bike : additional attribute is_geared
# Override a method description() in both

class Vehicle:
    def __init__(self,name,wheels):
        self.name=name
        self.wheels=wheels
    def description(self):
        print(f"Name of the vehicle:{self.name}")
        print(f"No.of wheels:{self.wheels}")

class Car(Vehicle):
    def __init__(self,name,wheels,fuel_type):
        super().__init__(name,wheels)
        self.fuel_type=fuel_type
    def description(self):
        super().description()
        print(f"Fuel Type:{self.fuel_type}")

class Bike(Vehicle):
    def __init__(self,name,wheels,is_geared):
        super().__init__(name,wheels)
        self.is_geared=is_geared
    def description(self):
        super().description()
        print(f"Is vehicle geared:{self.is_geared}")

b1=Bike("Royal Enfield",2,True)
c1=Car("Santro",4,"Petrol")
b1.description()
c1.description()

# 10. Polymorphism with Animals
# Base class Animal with method speak()
# Subclasses Dog , Cat , Cow override speak() with unique sounds
# Call speak() on each object in a loop
class Animal:
    def speak(self):
        pass

class Dog(Animal):
    def speak(self):
        print("Dog barks")

class Cat(Animal):
    def speak(self):
        print("Cat meows")

class Cow(Animal):
    def speak(self):
        super().speak()
        print("Cow moos")

d1=Dog()
d1.speak()
c1=Cat()
c1.speak()
cow1=Cow()
cow1.speak()