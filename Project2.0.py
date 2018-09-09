
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# In[2]:


df=pd.read_csv('https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv')
df1=pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv')


# In[3]:


df.head()


# ## 1. Get the metadata from the df

# In[7]:


# .info give the complete details of dataset (basicaly metadata of data set)
df.info()


# In[9]:


df1.info()


# ## Get the the row names

# In[14]:


# . index will give the row names and np.array to display in array formate.
np.array(df.index)


# In[15]:


np.array(df1.index)


# ## change the column names

# In[18]:


#.rename fun will use to change the column names here we hv to specify old and new column names. 
#We can specify multiple columns as well to rename with , separated 
df1.rename(columns={'Indicator': 'Indicator_Id'})


# ## Change column name permanety

# In[20]:


# inplace is used to make changes permanetly or not based on bool value we pass.
df1.rename(columns={'Indicator': 'Indicator_Id'},inplace=True)


# In[21]:


df1.head(2)


# ## Change multiple column names

# In[22]:


#.rename fun will use to change the column names here we hv to specify old and new column names. 
#We can specify multiple columns as well to rename with , separated 
df1.rename(columns={'Year': 'YearNew','Comments': 'Comments_N'},inplace=True)


# In[23]:


df1.head(2)


# ##  Sort based on one column

# In[29]:


# sort_values is used to sort based on columns specified and order 
df1.sort_values('YearNew',ascending=True).head(10)


# ## Multiple order

# In[30]:


# sort_values is used to sort based on columns specified and order 
df1.sort_values(['YearNew','Sex'],ascending=True).head(10)


# ## Make countryas the first column of the dataframe.

# In[31]:


# get the list of colummn names to list
cols = df1.columns.tolist()
cols


# In[32]:


# get the index of Column which we need to change the possion
# then concatinate cols based on ur requirement
n = int(cols.index('Country'))
cols = [cols[n]] + cols[:n] + cols[n+1:]
df1 = df1[cols]
df1.head(2)


# ## 9. Get the column array using a variable

# In[33]:


# selecting country col values into array
np.array(df1['Country'])


# ## 10. Get the subset rows 11, 24, 37

# In[34]:


# iloc is used to pick specific index loc
df1.iloc[[11,24,37]]


# ## 11. Get the subset rows excluding 5, 12, 23, and 56

# In[42]:


# first  rows which are in specified indexes
# using ~ to exclude the row which r there in test dataframe
test=df1.index.isin([5,12,23,56])


# In[44]:


df1[~test].head(20)


# In[2]:


users=pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
sessions =pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')
products =pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')
transactions =pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')


# In[46]:


users


# ## 12. Join users to transactions, keeping all rows from transactions and only matching
# rows from users (left join)

# In[71]:


# merge to join n tables based on key and how is used to specify kind of join
pd.merge(transactions,users,how='left', on ='UserID').head()


# ## Which transactions have a UserID not in users?

# In[50]:


# first will get the users table userid's which r in trasaction table and will exclude the userids from transaction table
transactions[~transactions.UserID.isin(users.UserID)]


# ## Join users to transactions, keeping only rows from transactions and users that
# match via UserID (inner join)

# In[72]:


# merge to join n tables based on key and how is used to specify kind of join
pd.merge(transactions,users,how='inner', on ='UserID')


# ## 15. Join users to transactions, displaying all matching rows AND all non-matching rows
# (full outer join)

# In[70]:


# merge to join n tables based on key and how is used to specify kind of join
pd.merge(transactions,users,how='outer', on ='UserID').head()


# ## 16. Determine which sessions occurred on the same day each user registered

# In[55]:


# merge to join n tables based on key and how is used to specify kind of join
pd.merge(sessions,users,how='inner', left_on='SessionDate', right_on='Registered').head()


# ## 17. Build a dataset with every possible (UserID, ProductID) pair (cross join)

# In[69]:


# since there is no common column in both the tables and will add one dummy column in both the table and selecting specific columns
# finaly we r droping temp key which we added
users['key'] = 1
products['key'] = 1
dfp=pd.merge(users, products, on='key').loc[:, ('UserID','ProductID')]
users.drop('key', axis=1, inplace=True)
products.drop('key', axis=1, inplace=True)
dfp.head()


# ## 18. Determine how much quantity of each product was purchased by each user

# In[82]:


# .fillna is used to fill the NaN with some value.
dfq=pd.merge(dfp, transactions, how='left' ,on=['UserID','ProductID']).loc[:, ('UserID','ProductID','Quantity')]

dfq.fillna('0').head()


# ## 19. For each user, get each possible pair of pair transactions (TransactionID1,TransacationID2)

# In[86]:


# self join to get the pair transactions
pd.merge(transactions,transactions,how='left',on='UserID').head()


# ## 20. Join each user to his/her first occuring transaction in the transactions table

# In[96]:


# .first will take the first row of the grouped values in the dataset
df_trn=transactions.groupby('UserID',as_index=False).first()

pd.merge(users,df_trn,how='left',on='UserID')


# ## 21. Test to see if we can drop columns

# In[104]:


# Deep copy
df_test=users.copy()
df_test


# In[105]:


df_test.drop('Gender',axis=1,inplace=True)


# In[106]:


df_test


# ## additional

# In[107]:


# to get NaN columns
users[users['Cancelled'].isna()]


# In[108]:


# to get not a NaN columns
users[users['Cancelled'].notna()]


# In[112]:


# to get the count of Gender
users.groupby('Gender').size()


# In[6]:


# aggregation
transactions.groupby(['UserID','ProductID']).agg({'Quantity': np.mean})

