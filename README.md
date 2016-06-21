Even number RDD

def even(x): return x % 2 == 0
	
rdd = sc.parallelize(range(100))

rdd_even = (rdd.filter(f) for f in (even))


Prime number RDD

def isprime(n):

    n = abs(int(n))
    
    if n < 2:
    
        return False
        
    if n == 2:
    
        return True
        
    if not n & 1:
    
        return False
        
    for x in range(3, int(n**0.5)+1, 2):
    
        if n % x == 0:
        
            return False
            
    return True

nums = sc.parallelize(xrange(100))

print nums.filter(isprime).count()

