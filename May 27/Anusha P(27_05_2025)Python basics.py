# VARIABLES, DATA TYPES, OPERATORS
import random
import math
# 1. Digit Sum Calculator Ask the user for a number and calculate the sum of its digits. Example: 753 → 7 + 5 + 3 = 15
n=input("Enter a number: ")
tot=0
for i in n:
    tot+=int(i)
print("Sum of its digit is: ",tot)

# 2. Reverse a 3-digit Number Input a 3-digit number and print it reversed. Input: 123 → Output: 321
n=int(input("Enter a 3-digit number"))
rev=0
while n>0:
    a = n%10
    rev = (rev*10)+a
    n//=10
print("Reversed number is ",rev)

# 3. Unit Converter Build a converter that takes meters and converts to:
# centimeters
# feet
# inches
m=float(input("Enter meter value"))
print("Centimeters: ",m*100)
print("Feet: ",m*3.2808)
print("Inches: ",m*39.37)

# 4. Percentage Calculator Input marks of 5 subjects and calculate total, average and percentage. Display grade based on the percentage.
marks=[]
print("Enter 5 subject marks:")
for i in range(5):
    n=float(input(f"Subject{i+1}:"))
    marks.append(n)
print("Total marks: ",sum(marks))
print("Average marks: ",sum(marks)/len(marks))
overall=int(input("Enter overall marks: "))
print("Percentage :",sum(marks)/overall)

# CONDITIONALS
# 5. Leap Year Checker A year is a leap year if it’s divisible by 4 and (not divisible by 100 or divisible by 400).
year=int(input("Enter year: "))
if year%400==0 or (year%4==0 and year%100!=0):
    print(year,"is a leap year")
else:
    print(year,"is not a leap year")

# 6. Simple Calculator Input two numbers and an operator ( + - * / ) and perform the operation using if...elif...else .
n1=int(input("Enter the first number: "))
n2=int(input("Enter the second number: "))
op=input("Enter the operator(+,-,*,/): ")
if op=='+':
    print(n1,op,n2,"=",n1+n2)
elif op=='-':
    print(n1,op,n2,"=",n1-n2)
elif op=='*':
    print(n1,op,n2,"=",n1*n2)
elif op=='/':
    if n2!=0:
        print(n1,op,n2,"=",n1/n2)
    else:
        print("Cannot divide by zero")
else:
    print("Invalid operator type")

# 7. Triangle Validator Given 3 side lengths, check whether they can form a valid triangle.
a=int(input("Enter side a:"))
b=int(input("Enter side b:"))
c=int(input("Enter side c:"))
if a+b>c and a+c>b and b+c>a:
    print("Valid triangle")
else:
    print("Invalid triangle")

# 8. Bill Splitter with Tip Ask total bill amount, number of people, and tip percentage. Show final amount per person.
bill=float(input("Enter the bill amount: "))
n=int(input("Enter the number of people: "))
tip_percent=float(input("Enter the tip percentage: "))
tip_amount=((tip_percent/100)*bill)/n
print("Final amount per person :",round(tip_amount,2))

# LOOPS
# 9. Find All Prime Numbers Between 1 and 100 Use a nested loop to check divisibility.
print("Prime Numbers between range 1 to 100")
for i in range(2,101):
    is_prime=True
    for j in range(2,int(i**0.5)+1):
        if i%j==0:
            is_prime=False
            break
    if is_prime:
        print(i)

# 10. Palindrome Checker Ask for a string and check whether it reads the same backward.
string=input("Enter a string: ")
if string==string[::-1]:
    print(string,"is a Palindrome")
else:
    print(string,"is not a Palindrome")

# 11. Fibonacci Series (First N Terms) Input n , and print first n terms of the Fibonacci sequence.
n=int(input("Enter the number of terms: "))
if n<=0:
    print("Please enter a positive integer")
else:
    dp=[0]*n
    if n>=1:
        dp[0]=1
    if n>=2:
        dp[1]=1
    for i in range(2, n):
        dp[i]=dp[i-1]+dp[i-2]
    print("Fibonacci series:", dp)

# 12. Multiplication Table (User Input) Take a number and print its table up to 10
n=int(input("Enter a number: "))
print("Multiplication Table: ")
for i in range(1,11):
    print(n,"x",i,"=",n*i)

# 13. Number Guessing Game
# Generate a random number between 1 to 100
# Ask the user to guess
# Give hints: "Too High", "Too Low"
# Loop until the correct guess
n=random.randint(1,100)
num=int(input("Guess the number between 1 to 100"))
if num==n:
    print("Correct guess")
while num!=n:
    if num == n:
        print("Correct guess")
        break
    elif num<n:
        print("Too Low")
    else:
        print("Too High")

# 14. ATM Machine Simulation
# Balance starts at 10,000
# Menu: Deposit / Withdraw / Check Balance / Exit
# Use a loop to keep asking
# Use conditionals to handle choices
balance=10000
while True:
    print("ATM system:")
    print("1.Deposit")
    print("2.Withdraw")
    print("3.Check Balance")
    print("4.Exit")
    ch=input("Enter your choice: ")
    if ch=='1':
        amount=float(input("Enter the deposit amount: "))
        balance+=amount
        print("Amount Deposited Successfully")
    elif ch=='2':
        amount=float(input("Enter the withdrawal amount: "))
        if amount<=balance:
            balance-=amount
            print("Current balance: ",balance)
            print("Amount Withdrawn Successfully")
        else:
            print("Insufficient balance")
    elif ch=='3':
        print("Balance amount: ",balance)
    elif ch=='4':
        print("Exiting ATM system")
        break
    else:
        print("Invalid Choice")

# 15. Password Strength Checker
# Ask the user to enter a password
# Check if it's at least 8 characters
# Contains a number, a capital letter, and a symbol
password=input("Enter a password:")
if len(password)<8:
    print("Password should contain at least 8 characters")
elif not any (char.isdigit() for char in password) :
    print("Password should contain at least one number")
elif not any (char.isupper() for char in password):
    print("Password should contain at least one capital letter")
elif not any (not char.isalnum() for char in password):
    print("Password should contain at least one special character")
else:
    print("Valid Password")

# 16. Find GCD (Greatest Common Divisor)
# Input two numbers
# Use while loop or Euclidean algorithm
n1=int(input("Enter first number: "))
n2=int(input("Enter second number: "))
print("GCD of two numbers: ",math.gcd(n1,n2))
