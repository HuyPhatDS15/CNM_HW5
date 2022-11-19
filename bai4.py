import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    key = record[0]
    if key == "a":
        for i in range(5):
            mr.emit_intermediate((record[1], i), (record[2], record[3]))
    if key == "b":
        for i in range(5):
            mr.emit_intermediate((i, record[2]), (record[1], record[3]))

def reducer(key, list_of_values):
    s = 0
    dict1 = {}
    dict2 = {}
    for v in list_of_values:
        if v[0] in dict1:
            dict2[v[0]] = v[1]
        else:
            dict1[v[0]] = v[1]
    for k in dict2.keys():
        s = s + dict2[k] * dict1[k]
    mr.emit((key[0], key[1], s))

if __name__ == '__main__':
    inputdata = open("matrix.json")
    mr.execute(inputdata, mapper, reducer)
