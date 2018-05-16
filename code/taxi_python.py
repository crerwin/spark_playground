import sys, time

def main(filename, command):
    if command == "total_passengers":
        print("getting total passenger count...")
        total_passengers(filename)
    elif command == "average_passenger_count":
        print("getting average passenger count...")
        average_passenger_count(filename)
    elif command == "trip_count":
        print("getting total trip count...")
        trip_count(filename)
    else:
        print("no command or invalid command specified")

def trip_count(filename):
    with open(filename) as f:
        next(f)
        next(f)
        rowcount = 0
        for line in f:
            rowcount += 1
        print(rowcount)

def total_passengers(filename):
    with open(filename) as f:
        next(f)
        next(f)
        totalpassengers = 0
        for line in f:
            fields = line.split(',')
            totalpassengers += int(fields[3])
        print(totalpassengers)

def average_passenger_count(filename):
    with open(filename) as f:
        next(f)
        next(f)
        totalpassengers = 0
        rowcount = 0
        for line in f:
            fields = line.split(',')
            totalpassengers += int(fields[3])
            rowcount += 1
        print(totalpassengers/float(rowcount))

if __name__ == "__main__":
    start_time = time.time()
    command = sys.argv[1]
    filename = sys.argv[2]
    main(filename, command)
    print("execution time (s): " + str((time.time() - start_time)))
