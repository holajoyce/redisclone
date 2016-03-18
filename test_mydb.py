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


def test_example1():
    r = RedisClone()
    r.process(["BEGIN"])
    r.process(["SET","a",30])
    r.process(["BEGIN"])
    r.process(["SET","a",40])
    r.process(["COMMIT"])
    assert r.process(["GET","a"]) == 40
    assert r.process(["ROLLBACK"])=="NO TRANSACTION"

 
def test_example2():
    r = RedisClone()
    r.process(["BEGIN"])
    r.process(["SET","a",10])
    assert r.process(["GET","a"])==10
    r.process(["BEGIN"])
    r.process(["SET","a",20])
    assert r.process(["GET","a"])==20
    r.process(["ROLLBACK"])
    assert r.process(["GET","a"])==10
    r.process(["ROLLBACK"])
    assert r.process(["GET","a"])=="NULL"
 
 
def test_example3():
    r = RedisClone()
    r.process(["SET","a",50])
    r.process(["BEGIN"])
    assert r.process(["GET","a"])==50
    r.process(["SET","a",60])
    r.process(["BEGIN"])
    r.process(["UNSET","a"])
    assert r.process(["GET","a"])=="NULL"
    r.process(["ROLLBACK"])
    assert r.process(["GET","a"])==60
    r.process(["COMMIT"])
    assert r.process(["GET","a"])==60
    
def test_example4():
    r = RedisClone()
    r.process(["SET","a",10])
    r.process(["BEGIN"])
    assert r.process(["NUMEQUALTO",10])==1
    r.process(["BEGIN"])
    r.process(["UNSET","a"])
    assert r.process(["NUMEQUALTO",10])==0
    r.process(["ROLLBACK"])
    assert r.process(["NUMEQUALTO",10])==1
    r.process(["COMMIT"])

    
def test_misc():
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
    
