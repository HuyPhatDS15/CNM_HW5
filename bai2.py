import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    table = record[0]
    order_id = record[1]
    data = record[2:]
    mr.emit_intermediate(order_id, (table, data))

def reducer(key, list_of_values):
    print(key, list_of_values)
    order_table, data_t = list_of_values[0]
    order = [order_table, key] + data_t

    for table, data in list_of_values[1:]:
        order = order + [table, key] + data
        mr.emit(order)
        order = [order_table, key] + data_t

if __name__ == '__main__':
    inputdata = open("records.json")
    mr.execute(inputdata, mapper, reducer)