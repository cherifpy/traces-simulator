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
    transfert_cost = 1/(b/(b*l + s))
    gain = n * l
    return transfert_cost + gain