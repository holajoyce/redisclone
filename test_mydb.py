'''
Created on Mar 16, 2016

@author: private
'''
from myDB import RedisClone


def test_db_set_and_unset():
    r = RedisClone()
    done = r.db_set(["SET","a",1])
    done2 = r.db_unset(["UNSET","a"])
    assert not r.db_hashmap
    assert not r.db_histogram


def test_db_set_and_reset():
    r = RedisClone()
    done = r.db_set(["SET","a",1])
    done2 = r.db_set(["SET","a",2])
    assert r.db_hashmap
    assert r.db_hashmap['a']==2
    assert r.db_histogram[2]==['a']


def test_db_set_set_count():
    r = RedisClone()
    done = r.db_set(["SET","a",1])
    done = r.db_set(["SET","b",1])
    numeq = r.process(["NUMEQUALTO",1])
    assert numeq ==2
    assert 'a' in r.db_hashmap and 'b' in r.db_hashmap
    assert r.db_histogram[1]==['a','b']



def test_db_transaction_with_commit():
    r = RedisClone()
    some_command = r.process(["SET","a",5])
    assert r.db_hashmap["a"] == 5
     
    had_began =r.process(["BEGIN"])
    had_set = r.process(["SET","a",1])
    assert r.db_hashmap["a"] == 5
    result = r.process(["GET","a"])
    assert result == 1
     
    r.process(["BEGIN"])
    r.process(["SET","a",3])
    assert r.process(["GET", "a"]) == 3
    assert r.db_hashmap["a"] == 5
    
    r.process(["COMMIT"])
    assert r.db_hashmap["a"] == 3
     
    r.process(["COMMIT"])
    assert r.db_hashmap["a"] == 1


def test_db_transaction_with_rollback():
    r = RedisClone()
    some_command = r.process(["SET","a",5])
    assert r.db_hashmap["a"] == 5
     
    had_began =r.process(["BEGIN"])
    had_set = r.process(["SET","a",1])
    assert r.db_hashmap["a"] == 5
    result = r.process(["GET","a"])
    assert result == 1
     
    r.process(["BEGIN"])
    r.process(["SET","a",3])
    assert r.process(["GET", "a"]) == 3
    assert r.db_hashmap["a"] == 5
    
    r.process(["ROLLBACK"])
    assert r.db_hashmap["a"] == 5
    
    assert r.process(["GET","a"])==1    
     
    r.process(["COMMIT"])
    assert r.db_hashmap["a"] == 1


def test_db_transaction_with_bad_rollback():
    r = RedisClone()
    some_command = r.process(["SET","a",5])
    assert r.db_hashmap["a"] == 5
     
    had_began =r.process(["BEGIN"])
    had_set = r.process(["SET","a",1])
    assert r.db_hashmap["a"] == 5
    result = r.process(["GET","a"])
    assert result == 1
     
    r.process(["BEGIN"])
    r.process(["SET","a",3])
    assert r.process(["GET", "a"]) == 3
    assert r.db_hashmap["a"] == 5
    
    r.process(["ROLLBACK"])
    assert r.db_hashmap["a"] == 5
    
    assert r.process(["GET","a"])==1    
     
    r.process(["COMMIT"])
    assert r.db_hashmap["a"] == 1
    
    assert r.process(["ROLLBACK"])=="NO TRANSACTION"

def test_everything_in_tranasactions1():
    r = RedisClone()

    had_began =r.process(["BEGIN"])
    had_set = r.process(["SET","a",10])
    assert r.process(["GET","a"])==10
     
    r.process(["BEGIN"])
    r.process(["SET","a",20])
    assert r.process(["GET", "a"]) == 20
    
    r.process(["ROLLBACK"])
    
    assert r.process(["GET","a"]) == 10
    
    r.process(["ROLLBACK"])
    assert r.process(["GET","a"]) == "NULL"
    
    assert r.process(["ROLLBACK"])=="NO TRANSACTION"


def test_input_from_text_file():
    msg = u"""SET a 5
    GET a
    BEGIN
    SET a 1
    GET a
    ROLLBACK
    GET a
    END
    """
    r = RedisClone()
    r.readlines(msg)
    
