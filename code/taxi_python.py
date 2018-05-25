import sys
import time
import datetime


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
    elif command == "list_vendor_ids":
        print("listing vendor ids...")
        list_vendor_ids(filename)
    elif command == "revenue_by_vendor":
        print("listing revenue by vendor...")
        revenue_by_vendor(filename)
    elif command == "slow_join":
        slow_join(filename)
    else:
        print("invalid command specified: " + command)


def slow_join(filename):
    print("getting brooklyn location codes")
    brooklyn_codes = []
    with open("data/taxi_zone_lookup.csv") as tzf:
        next(tzf)
        for line in tzf:
            fields = line.rstrip().split(',')
            if fields[1] == '"Brooklyn"':
                brooklyn_codes.append(fields[0])
    print(str(len(brooklyn_codes)) + " brooklyn codes loaded.")
    print("loading " + filename + " into memory")
    with open(filename) as f:
        content = f.readlines()
        del content[0]
        del content[0]
        print(str(len(content)) + " lines in content")
        vendors = dict()
        print("getting vendor who transported fewer"
              + " passengers on trips over 5 miles")
        print(str(len(content)) + " lines in content")
        for line in content:
            fields = line.rstrip().split(',')
            vendorid = fields[0]
            passenger_count = float(fields[3])
            trip_distance = float(fields[4])
            if trip_distance > 5.0:
                if vendorid not in vendors:
                    vendors[vendorid] = passenger_count
                else:
                    vendors[vendorid] += passenger_count
        vendor_with_fewer = 0
        fewest = 0
        for key in vendors:
            if fewest == 0 or vendors[key] < fewest:
                fewest = vendors[key]
                vendor_with_fewer = key
        print(vendors)
        print("vendor ID: " + vendor_with_fewer)
        print("For that vendor, getting list of trips where drop-off occurred"
              + " between 6pm and 6am in Brooklyn and determining the most"
              + " popular drop off location")
        filtered_rides = []
        drop_offs = dict()
        print(str(len(content)) + " lines in content")
        for line in content:
            fields = line.rstrip().split(',')
            if vendor_with_fewer == fields[0]:
                drop_off = fields[2]
                drop_off_location_id = fields[8]
                drop_off_time = datetime.datetime.strptime(
                                                    drop_off,
                                                    "%Y-%m-%d %H:%M:%S"
                                                  ).time()
                if (drop_off_time > datetime.time(18)
                        or drop_off_time < datetime.time(6)):
                    if drop_off_location_id in brooklyn_codes:
                        filtered_rides.append(line)
                        if drop_off_location_id in drop_offs:
                            drop_offs[drop_off_location_id] += 1
                        else:
                            drop_offs[drop_off_location_id] = 1
        most_popular_drop_off_id = ""
        most_drop_offs = 0
        for key in drop_offs:
            if drop_offs[key] > most_drop_offs:
                most_drop_offs = drop_offs[key]
                most_popular_drop_off_id = key
        print("most popular drop off: " + most_popular_drop_off_id + " with "
              + str(most_drop_offs) + " drop offs.")
        total_trips = 0
        total_tips = 0.0
        total_revenue = 0.0
        print("filtered_rides: " + str(len(filtered_rides)))
        print("writing filtered rides to file")
        fw = open("filtered.csv", "w+")
        for line in filtered_rides:
            fw.write(line)
        fw.close()
        print("of trips with that drop off, getting credit card tips")
        for line in filtered_rides:
            fields = line.rstrip().split(',')
            payment_type = fields[9]
            if payment_type == "1" and fields[8] == most_popular_drop_off_id:
                total_trips += 1
                total_tips += float(fields[13])
                total_revenue += float(fields[16])
        print(total_trips, total_tips, total_revenue,
              total_tips/(total_revenue-total_tips))


def revenue_by_vendor(filename):
    vendors = dict()
    with open(filename) as f:
        next(f)
        next(f)
        for line in f:
            fields = line.rstrip().split(',')
            vendorid = fields[0]
            total = float(fields[16])
            if vendorid not in vendors:
                vendors[vendorid] = total
            else:
                vendors[vendorid] += total
        print(vendors)


def list_vendor_ids(filename):
    with open(filename) as f:
        next(f)
        next(f)
        vendorids = []
        for line in f:
            fields = line.split(',')
            if fields[0] not in vendorids:
                vendorids.append(fields[0])
        print(vendorids)


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
