import sqlite3
import cPickle as pickle
import time

# Persistent set class, use like a set object. Saves to SQLITE3 DB
class PersistentSet:
    def __init__(self, db_file_name):
        self.__conn      = sqlite3.connect(db_file_name, check_same_thread=False)
        self.__tablename = self.__class__.__name__

        self.__conn.execute("CREATE TABLE IF NOT EXISTS %s (hash INTEGER PRIMARY KEY, key BLOB);" % self.__tablename)
        self.__conn.commit()
        return

    def __pack(self, value):
        return sqlite3.Binary(pickle.dumps(value, -1))

    def __unpack(self, value):
        return pickle.loads(str(value))

    def add(self, key):
        if not self.exists(key):
            self.__conn.execute("REPLACE INTO %s (hash, key) VALUES (?, ?);" % self.__tablename, (hash(key), key) )
            self.__conn.commit()
        return True

    def get(self, key):
        cursor = self.__conn.execute("SELECT key FROM %s WHERE hash = ?" % self.__tablename, (hash(key),) )
        return cursor.fetchone()[0]

    def exists(self, key):
        try:
            cursor = self.__conn.execute("SELECT 1 FROM %s WHERE hash = ?" % self.__tablename, (hash(key),) )
            return cursor.fetchone()[0] == 1
        except:
            return False

    def remove(self, key):
        try:
            self.__conn.execute("DELETE FROM %s WHERE hash = ?;" % self.__tablename, (hash(key),) )
            self.__conn.commit()
            return True
        except:
            return False

    def debug(self):
        cursor = self.__conn.execute("SELECT * FROM %s WHERE 1" % self.__tablename)
        return cursor.fetchall()

    def __getitem__(self, key):
        return self.get(key)

    def __contains__(self, key):
        return self.exists(key)

# Persistent dictionary class, use like a dictionary object. Saves to SQLITE3 DB

class PersistentDict:
    def __init__(self, db_file_name, commit=False):
        self.__conn      = sqlite3.connect(db_file_name)
        self.__tablename = self.__class__.__name__
        self.__commit    = commit

        # Create database and index
        self.__conn.execute('CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY, hash INTEGER, key BLOB, value BLOB);' %(self.__tablename))
        self.__conn.execute('CREATE INDEX IF NOT EXISTS %s_index ON %s(hash);' %(self.__tablename, self.__tablename))
        self.commit()
        return

    def __pack(self, value):
        return sqlite3.Binary(pickle.dumps(value, -1))

    def __unpack(self, value):
        return pickle.loads(str(value))

    def commit(self):
        if self.__commit is True:
            self.__conn.commit()

    def get_id(self, key):
        cursor = self.__conn.execute("SELECT id, key FROM %s WHERE hash = ?;" % self.__tablename, (hash(key), ))
        for i, k in cursor:
            if self.__unpack(k) == key:
                return i
        raise KeyError(key)

    def get(self, key):
        cursor = self.__conn.execute("SELECT key, value FROM %s WHERE hash = ?;" % self.__tablename, (hash(key), ))
        for k, v in cursor:
            if self.__unpack(k) == key:
                return self.__unpack(v)
        raise KeyError(key)

    def set(self, key, value):
        value_packed = self.__pack(value)
        try:
            row_id = self.get_id(key)
            cursor = self.__conn.execute("UPDATE %s SET value = ? WHERE id = ?;" % self.__tablename, (value_packed, row_id))
        except KeyError:
            key_packed = self.__pack(key)
            cursor = self.__conn.execute("INSERT INTO %s (hash, key, value) VALUES (?, ?, ?);" % self.__tablename, (hash(key), key_packed, value_packed))

        assert cursor.rowcount == 1, "Row count not 1"
        self.commit()

    def iterkeys(self):
        cursor = self.__conn.execute("SELECT id, key FROM %s ORDER BY id;" % self.__tablename)
        for i, k in cursor:
            # keys.append(self.__unpack(k))
            yield self.__unpack(k)

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        row_id = self.get_id(key)
        cursor = self.__conn.execute("DELETE FROM %s WHERE id = ?;" % self.__tablename, (row_id,))
        if cursor.rowcount <= 0:
            raise KeyError(key)
        self.commit()

    def __contains__(self, key):
        try:
            self.get_id(key)
            return True
        except KeyError:
            return False

    def keys(self):
        return list(self.iterkeys())



'''
db = PersistentDict('test.db')
db['test1'] = 'testing'
assert 'test1' in db, "Contains function error (exists)"
assert 'notfound' not in db, "Contains function error (DNE)"

# Benchmark
start = time.clock()

for i in xrange(10000):
    db['k_'+str(i)] = 'v_'+str(i)

elapsed = time.clock() - start
print "Finished inserting in", elapsed

start = time.clock()

for i in xrange(10000):
    'k_'+str(i) in db

elapsed = time.clock() - start
print "Finished looking up in", elapsed

start = time.clock()

for i in xrange(10000):
    del db['k_'+str(i)]

elapsed = time.clock() - start
print "Finished deleting in", elapsed

db = PersistentSet('test.db')

db.add('test_insert')
assert db['test_insert'] == 'test_insert', "Class fetch function failed"
db.add('test_insert')
db.add('test_in_check')
assert 'test_in_check' in db, "Class exists function failed (exists)"
assert 'test_in_check2' not in db, "Class exists function failed (not exists)"

print db.debug()

# Benchmark
start = time.clock()
db = PersistentSet('test2.db')

for i in xrange(100):
    db.add(i)

elapsed = time.clock() - start
print "Finished inserting in", elapsed

start = time.clock()

for i in xrange(100):
    i in db

elapsed = time.clock() - start
print "Finished looking up in", elapsed

start = time.clock()

for i in xrange(100):
    db.remove(i)

elapsed = time.clock() - start
print "Finished delete in", elapsed

print db.debug()
'''