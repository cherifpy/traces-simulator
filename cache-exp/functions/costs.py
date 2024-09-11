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

def transfertTime(b, l, s):
    """
        params:
            b = bandwidth
            l = latency
            s = dataset size

        this cost fonction try to minimise the transfert cost
    """

    bandwith_in_bits = b*1024*1024*8
    size_in_bits = s*1024*8
    latency_in_s = l/1000

    return latency_in_s + (size_in_bits/bandwith_in_bits)

def energieAndTransfertCost(b,l,s):
    pass


def nodeImportanceV2(id_node, b, s, graphe_infos, nb_requests):
    """
        dans cette fonction je me base sur la fonction node importance en ajoutant le nombre de requete qui passe par ce noued
        possible de saturé le noeud mais je dois voir commennt regler ce truc
    """

    sum = 0
    for l in graphe_infos[id_node]:
        if l > 0: sum += transfertTime(b,l,s)

    return sum*nb_requests

def nodeImportance(id_node, b,s, graphe_infos):

    sum = 0
    for l in graphe_infos[id_node]:
        if l > 0: sum += transfertTime(b,l,s)

    return sum


def fobjectif():
    """
        - dans cette fonction je dois tenir en contre
            - le nombre de voisin du noeud
            - le nombre de voisin en commun avec la source
            - l'espace de stockage

    
        - le nombre de demande passée par le noued
        - aussi parler sur le truc de julia qui m'as donné un papier sur la conso energitique
        selon la distance parcourue par la requet
    """
    pass




