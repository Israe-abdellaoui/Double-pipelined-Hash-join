import csv
import os
import sys
import pandas as pd
import threading
import psycopg2
from sqlalchemy import create_engine



class HashTable:  
    def __init__(self):
        self.MAX = 10000
        self.arr = [None for i in range(self.MAX)]
        
    def get_hash(self, key):
        hash = 0
        for char in key:
            hash += ord(char)
        return hash % self.MAX
    
    def get(self, key):
        h = self.get_hash(key)
        return self.arr[h]   
      
    def put(self, key, val):
        h = self.get_hash(key)
        self.arr[h] = val    
    def findkey(self, key):
        h = self.get_hash(key)
        if self.arr[h] == None:
            return False
        return True
        
## def __delitem__(self, key):
#   h = self.get_hash(key)
# self.arr[h] = None  





def probAndInsert(fil,t1,t2,fl): 
    conn_string = 'postgresql://postgres:1234@localhost/Hash'
    db = create_engine(conn_string)
    conn = db.connect()
    
    f=fl
    if(f==1):
        fil="myFile0.csv"
    else:
        fil="myFile1.csv"
    with open(fil) as fil:
        csv_reader = csv.reader(fil, delimiter=',')
        row = next(csv_reader)
        index1={row[0]}
        column2={row[1]}
          
#prob
    #hashIndex=t1.get_hash('index1')
    if(t2.findkey('index1')!=False):
        
        d = {'id': [index1], 'firstname': [column2], 'city': [t2.get('index1')]}
        df = pd.DataFrame(data=d)
        df.to_sql('HashJoin',con=conn, if_exists='append', index=False)
        conn = psycopg2.connect(conn_string)
        conn.autocommit = True
        #cursor = conn.cursor()
#insert
    #array.append(column2)
    #array.append(column3)
    #array.append(column4)
    #if(f==1):
        #array.append(column5)
        #array.append(column6)
    t1.put('index1',column2)
    return row     
  

def doublePipe():
    t1=HashTable()
    t2=HashTable()
    flag =True  
    count=0
    count2=0
    with open('myFile0.csv') as file1:
        csv_reader = csv.reader(file1, delimiter=',')
        row = next(csv_reader)
    with open('myFile1.csv') as file2:
        csv_reader2 = csv.reader(file2, delimiter=',') 
        row2 =next(csv_reader2)
    while(row!=None and row2!=None):
        #row = next(csv_reader)
        #row2 =next(csv_reader2)
    #while f.hasnext() and g.hasnext():       
        if (flag== True):
            fl=1
            row=probAndInsert(file1,t1,t2,fl)
            x=threading.Thread(target=probAndInsert,args=(file1,t1,t2,fl))
            x.start()
            count += 1
            if (count==10000):
                #flush to dik
                t1=[0]*10000
        else: 
            fl=2  
            row2=probAndInsert(file2,t2,t1,fl)     
            y=threading.Thread(target=probAndInsert,args=(file2,t2,t1,fl))
            y.start()
            count2 += 1
            if (count2==10000):
                #flush to dik
                t2=[0]*10000
        flag = not flag


def main():
    #create a file reader for myFile0.csv
    #file1 = pd.read_csv("myFile0.csv")
    #file1 = pd.read_csv('myFile0.csv', names=['id', 'firstname', 'lastname', 'e-mail1', 'e-mail2', 'profession'])
    
    #create a file reader for myFile1.csv
    #file2 = pd.read_csv("myFile1.csv")
    #file2 = pd.read_csv('myFile1.csv', names=['id', 'city', 'country', 'birthdate'])
        
    #t1=HashTable()
    #t2=HashTable()
    #connection to the database 
    
    doublePipe()
    
    
    
    



if __name__ == "__main__":
    main()