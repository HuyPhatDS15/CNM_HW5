import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    person = record[0]
    friend = record[1]
    mr.emit_intermediate((person, friend), 1)
    mr.emit_intermediate((friend, person), 1)

def reducer(key, list_of_values):
    if len(list_of_values) < 2:
        mr.emit(key)

if __name__ == '__main__':
    inputdata = open("friends.json")
    mr.execute(inputdata, mapper, reducer)