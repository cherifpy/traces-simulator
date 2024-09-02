def transefrtWithGain(b, l, s, n):
    """
        params:
            b = bandwidth
            l = latency
            s = dataset size
            n = popularity

        this cost fonction try to minimise the transfert cost will maximising the gain

        the transfert cost is the time to transfert the data from Src to distiantion: here the src ith the node where the data may be evicted
        the gain is the latency times the popularity of a data on a node this mean the number of requete a node recieved for the data
    """
    bandwith_in_bits = b*1024*1024*8
    size_in_bits = s*1024*8
    latency_in_s = l/1000

    transfert_cost = 1/(bandwith_in_bits/(bandwith_in_bits*latency_in_s + size_in_bits))
    gain = n * latency_in_s
    return transfert_cost + gain