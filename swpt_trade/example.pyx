import math

cpdef double dist((double, double) point1, (double, double) point2):
    cdef double x = (point1[0] - point2[0]) ** 2
    cdef double y = (point1[1] - point2[1]) ** 2
    return math.sqrt(x + y)
