# LISTS
# 1. List of Squares: Create a list of squares of numbers from 1 to 20.
squares=[]
for i in range(1,21):
    squares.append(i**2)
print(squares)

# 2. Second-Largest Number: Find the second-largest number in a list without using sort().
arr=[1,3,5,4,8,6,10,96,69,56,73]
maxi=max(arr)
second_maxi=float('-inf')
for i in range(len(arr)):
    if arr[i]!=maxi and arr[i]>second_maxi:
        second_maxi=arr[i]
print(second_maxi)

# 3. Remove Duplicates: Write a program to remove all duplicate values from a list while preserving order.
arr=[48,56,48,45,67,59,74,58,59,64,45]
res=[]
for i in arr:
    if i not in res:
        res.append(i)
print(res)
# arr1=[]
# i=0
# while i<len(arr):
#     if arr[i] not in arr1:
#         arr1.append(arr[i])
#         i+=1
#     if arr[i] in arr1:
#         arr.pop(i)
# print(arr)

# 4. Rotate List: Rotate a list to the right by k steps.
# Example: [1, 2, 3, 4, 5] rotated by 2 → [4, 5, 1, 2, 3]
arr=[1,2,3,4,5]
k=2
n=len(arr)
k%=n
print("Rotated array",arr[n-k:]+arr[:k+1])

# 5. List Compression: From a list of numbers, create a new list with only the even numbers doubled.
arr=[1,2,3,4,5]
new_arr=[]
for i in arr:
    if i%2==0:
        new_arr.append(i*2)
print(new_arr)

# --------------------------------------------------------------------------------------------------------

# TUPLES
# 6. Swap Values: Write a function that accepts two tuples and swaps their contents.
tup1=(1,2,3)
tup2=(4,5,6)
tup1,tup2=tup2,tup1
print(tup1)
print(tup2)

# 7. Unpack Tuples: Unpack a tuple with student details: (name, age, branch, grade) and print them in a sentence.
tup=('Anusha',21,'AI&DS','A+')
print("Name of the student: ",tup[0])
print("Age: ",tup[1])
print("Branch: ",tup[2])
print("Grade: ",tup[3])

# 8. Tuple to Dictionary: Convert a tuple of key-value pairs into a dictionary.
# Example: (("a", 1), ("b", 2)) → {"a": 1, "b": 2}
tup=(('a',1),('b',2))
dic=dict(tup)
print(dic)

# -----------------------------------------------------------------------------------

# SETS
# 9. Common Items: Find the common elements in two user-defined lists using sets.
list1=[2,3,4,5,1]
list2=[1,3,5,7,9]
print(set(list1).intersection(set(list2)))

# 10. Unique Words in Sentence: Take a sentence from the user and print all unique words.
sentence="Red lorry blue lorry"
words=sentence.split()
print(set(words))

# 11. Symmetric Difference: Given two sets of integers, print elements that are in one set or the other, but not both.
a={1,2,3,4,5}
b={1,3,5,7,9}
a.symmetric_difference(b)

# 12. Subset Checker: Check if one set is a subset of another.
a={1,2,3}
b={1,2}
print("IS a subset of b?",a.issubset(b))
print("IS b subset of a?",b.issubset(a))

# -------------------------------------------------------------------------------------------------------

# DICTIONARIES
# 13. Frequency Counter: Count the frequency of each character in a string using a dictionary.
string=input("Enter a string: ")
dic={}
for i in string:
    if i in dic:
        dic[i]+=1
    else:
        dic[i]=1
print("Frequency of each character:",dic)

# 14. Student Grade Book: Ask for names and marks of 3 students. Then ask for a name and display their grade
# ( >=90: A , >=75: B , else C).
dic={}
for i in range(3):
    name=input("Enter the name of the student: ")
    marks=int(input("Enter marks: "))
    if marks>=90:
        grade='A'
    elif marks>=75:
        grade='B'
    else:
        grade='C'
    dic[name]=grade
val=input("Enter the name of the student to know the grade: ")
if val in dic:
    print(dic[val])
else:
    print("Student not found")

# 15. Merge Two Dictionaries: Merge two dictionaries. If the same key exists, sum the values.
dic1={'A':5,'B':10,'C':15}
dic2={'A':25,'D':50,'F':75}
for i in dic2:
    if i in dic1:
        dic1[i]+=dic2[i]
    else:
        dic1[i]=dic2[i]
print(dic1)

# 16. Inverted Dictionary: Invert a dictionary’s keys and values. Input: {"a": 1, "b":2} → Output: {1: "a", 2: "b"}
dic={'a':1,'b':2}
inv={}
for key,val in dic.items():
    if val not in inv:
        inv[val]=[key]
    else:
        inv[val].append(key)
print("Inverted dictionary",inv)

# 17. Group Words by Length: Input a list of words.
# Create a dictionary where the key is word length and the value is a list of words of that length.
words=['apple','mango','orange','grapes','banana']
dic={}
for word in words:
    if len(word) not in dic:
        dic[len(word)]=[word]
    else:
        dic[len(word)].append(word)
print(dic)

