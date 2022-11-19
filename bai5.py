import threading

x = 0

def Function1():
    global x
    x += 1

def Thread1():
    for r in range(100000):
        Function1()

def Thread2():
    global x
    x = 0
    t1 = threading.Thread(target=Thread1)
    t2 = threading.Thread(target=Thread1)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

if __name__ == "__main__":
    for i in range(10):
        Thread2()
        print("Iteration {0}: x = {1}".format(i, x))
