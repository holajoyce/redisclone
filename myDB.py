#!/usr/bin/python
'''
Created on Mar 16, 2016

@author: private
'''
import io
import sys
msg = u"""SET a 5
GET a
BEGIN
SET a 1
GET a
ROLLBACK
GET a
END
"""

class Transaction(object):
    
    def __init__(self,db_hashmap,db_histogram):
        self.transaction=[]
        self.db_hashmap = db_hashmap
        self.db_histogram = db_histogram
        
    def __str__(self):
        ",".join(self.transaction)
        
    

class RedisClone(object):
    
    def __init__(self):
        self.db_hashmap = {}
        self.db_histogram ={}
        
        # write ahead log
        self.db_transactions = []
        
    #--------some methods for working with histogram-----
    def _add_to_histogram(self,db_histogram,val,key,prev_val=None):
        if prev_val and not prev_val is val:
            if len(db_histogram[prev_val])==1:
                db_histogram.pop(prev_val)
            else:
                db_histogram[prev_val].remove(key)
        if val in db_histogram:
            db_histogram[val].append(key)
            if len(db_histogram[val]) > 1: 
                sorted(db_histogram)
        else:
            db_histogram[val] = [key]
        return db_histogram

    def _remove_from_histogram(self,db_histogram,val,key):
        if len(db_histogram[val])==1:
            db_histogram.pop(val)
        else:
            db_histogram[val].remove(key)
        return db_histogram
    
    #-----------------------
    def db_set(self,instruction,t=None):
        if not t:  # if not a transaction
            self.db_hashmap,self.db_histogram =  self.db_set_helper(instruction, self.db_hashmap,self.db_histogram)
        else:
            t.db_hashmap, t.db_histogram = self.db_set_helper(instruction,t.db_hashmap, t.db_histogram)
        return True
    
    def db_set_helper(self,instruction,db_hashmap, db_histogram):
        key = instruction[1]
        val = instruction[2]
        if len(instruction)==3:
            # if not in hm already add & add to histogram
            if not key in db_hashmap:
                db_histogram = self._add_to_histogram(db_histogram,val, key)
            else: # if in hashm, update & update histogram
                db_histogram = self._add_to_histogram(db_histogram,val, key, db_hashmap[key])
            db_hashmap[key] = val
        return db_hashmap,db_histogram
    
    #-----------------------
    def db_unset(self,instruction, t=None):
        if not t:   # if not a transaction
            self.db_hashmap, self.db_histogram = self.db_unset_helper(instruction, self.db_hashmap,self.db_histogram)
        else:      
            t.db_hashmap, t.db_histogram = self.db_unset_helper(instruction,t.db_hashmap, t.db_histogram)
        return True
    
    def db_unset_helper(self,instruction,db_hashmap,db_histogram):
        if len(instruction)==2:
            key = instruction[1]
            val = db_hashmap.pop(key, None)
            db_histogram = self._remove_from_histogram(db_histogram,val,key)
        return db_hashmap, db_histogram

    #-----------------------
    def db_numequalto(self,instruction,transaction=None):
        processed = 0
        if not transaction: 
            processed = self.db_numequalto_helper(instruction, self.db_histogram)
        else:               
            processed = self.db_numequalto_helper(instruction, transaction.db_histogram)
        return processed
    def db_numequalto_helper(self,instruction,db_histogram):
        if len(instruction)==2:
            return len(db_histogram[instruction[1]])
        return 0
    
    #-----------------------
    def db_get(self,instruction,transaction=None):
        if transaction:     return self.db_get_helper(instruction, transaction.db_hashmap)
        else:               return self.db_get_helper(instruction, self.db_hashmap)
        
    def db_get_helper(self,instruction,db_hashmap):
        if len(instruction)==2:
            if instruction[1] in db_hashmap:  
                return db_hashmap[instruction[1]]
            return "NULL"
        return None
    
    
    #-----------------------
    def process_db_instructions(self,instruction,transaction=None):
        processed = False
        if instruction[0]=="SET":           processed = self.db_set(instruction,transaction)
        elif instruction[0]=="GET":         processed = self.db_get(instruction,transaction)
        elif instruction[0]=="UNSET":       processed = self.db_unset(instruction,transaction)
        elif instruction[0]=="NUMEQUALTO":  processed = self.db_numequalto(instruction,transaction) 
        return processed
        
    # parse the instruction
    def process(self,instruction):
        processed = False
        if instruction[0]=="BEGIN":             
            processed = self.db_begin()
        elif instruction[0]=="COMMIT":
            processed = self.db_commit()
        elif instruction[0]=="ROLLBACK":
            processed = self.db_rollback()
        else:
            if (not self.db_transactions):
                processed = self.process_db_instructions(instruction, None)
            elif instruction[0]=="SET" or instruction[0]=="GET" or instruction[0]=="UNSET" or instruction[0]=="NUMEQUALTO": 
                t = self.db_transactions.pop()
                t.transaction.append(instruction)
                self.db_transactions.append(t)
                processed = self.process_db_instructions(instruction, t)
        return processed
    
    # put lines into write ahead log
    def db_begin(self):
        self.db_transactions.append(Transaction(self.db_hashmap.copy(),self.db_histogram.copy()))
        return True

    # the whole item is not commited and removed
    def db_rollback(self):
        if not self.db_transactions:
            return "NO TRANSACTION"
        self.db_transactions.pop()
        return True
    
    # remove lines from write ahead log
    def db_commit(self):
        t = self.db_transactions.pop()
        self.db_hashmap, self.db_histogram = t.db_hashmap, t.db_histogram
        return True
    
    def read_a_line(self,buf):
        line = buf.readline().strip()
        if line: 
            instruction = line.split()
            result = self.process(instruction)
            if not isinstance(result, bool): print(result)
        return line
    
    def readlines(self,msg):
        buf = io.StringIO(msg)
        line = self.read_a_line(buf)
        while line and not line=="END":
            line = self.read_a_line(buf)
        print(self)


def main():
    r = RedisClone()
    
    # read from file
    sysargv = sys.argv
    if(len(sysargv)>1):
        s =''
        with open(sysargv[1]) as f: 
            s = f.read()
        r.readlines(msg)
    
    else: # read from input
        line = raw_input()
        while line and not line=="END":
            instruction  = line.split()
            result = r.process(instruction)
            if not isinstance(result, bool): print(result)
            line = raw_input()


if __name__ == "__main__":
    main()

