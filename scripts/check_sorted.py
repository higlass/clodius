import sys

prev_val = 0
prev_val2 = 0
counter = 0

for line in sys.stdin:
    parts = line.split()
    curr_val = int(parts[0])
    curr_val2 = int(parts[1])

    val1 = int(parts[0])
    val2 = int(parts[1])

    if val1 > val2:
        print >>sys.stderr, "Not triangular:", counter
        sys.exit(1)

    if curr_val < prev_val:
        print >>sys.stderr, "Not sorted, line:", counter
        sys.exit(1)
    elif curr_val == prev_val:
        if (curr_val2 < prev_val2):
            print >>sys.stderr, "Not sorted, line:", counter
            sys.exit(1)
    
    prev_val = curr_val
    prev_val2 = curr_val2
    counter += 1

    if counter % 1000000 == 0:
        print "counter:", counter, prev_val, curr_val
