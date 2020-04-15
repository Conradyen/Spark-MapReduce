# Spark-MapReduce


## 1. Find common friend list of every pair of user

implement in:

```
commonFriend()
```
Find total number of common friends for any possible friend pairs.

For example,
Alice’s friends are Bob, Sam, Sara, Nancy Bob’s friends are Alice, Sam, Clara, Nancy Sara’s friends are Alice, Sam, Clara, Nancy

As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. 

The input format:
<User><TAB><Friends>

Output: 
```
<User_A>, <User_B><TAB><Common Friend Number> 
 ```

## 2. top-10 friend pairs by their total number of common friends

implement in:

```
topTenCommonFriend()
```

user data input format:
column1 : userid
column2 : firstname 
column3 : lastname 
column4 : address 
column5: city 
column6 :state 
column7 : zipcode 
column8 :country 
column9 :username
column10 : date of birth.

output format:
```
<Total number of Common Friends><TAB><First Name of User A><TAB><Last Name of User A> <TAB><address of User A><TAB><First Name of User B><TAB><Last Name of User B><TAB>
<address of User B>
```

## 3.Yelp dataset userID and rating in Standford area 

implement in:

```
reviewInStandford()
```

Data set info:
The data set comprises of three csv files, namely user.csv, business.csv and review.csv.  

Business.csv contains the following columns "business_id"::"full_address"::"categories"
'business_id': (a unique identifier for the business)
'full_address': (localized address), 
'categories': [(localized category names)]  

review.csv contains the following columns "review_id"::"user_id"::"business_id"::"stars"
 'review_id': (a unique identifier for the review)
 'user_id': (the identifier of the reviewed business), 
 'business_id': (the identifier of the authoring user), 
 'stars': (star rating, integer 1-5),the rating given by the user to a business

user.csv contains the following columns "user_id"::"name"::"url"
user_id': (unique user identifier), 
'name': (first name, last initial, like 'Matt J.'), this column has been made anonymous to preserve privacy 
'url': url of the user on yelp

output format:

```
<User id><Rating>
```


## 4.List the  business_id , full address and categories of the Top 10 businesses according to the average ratings.  

implement in:

```
avgRrating()
```

output format:

```
<business id><Tab><address><Tab><categories><Tab><avg rating>
```


