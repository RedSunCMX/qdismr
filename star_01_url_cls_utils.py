# -*- coding: utf-8 -*-
# [Author]: Chen Mingxing, chenmingxing@360.cn
# [Data]: 2013/11/12
import networkx as nx
import numpy as np
from scipy.cluster.vq import vq, kmeans
import scipy.linalg as linalg
import sys, time

class spectralClusU:

    def __init__(self):
        pass

    ################################################################################
    def getNormLaplacian(self, W):
        # """input matrix W=(w_ij)
        # "compute D=diag(d1,...dn)
        # "and L=D-W
        # "and Lbar=D^(-1/2)LD^(-1/2)
        # "return Lbar
        # """
        d = [np.sum(row) for row in W]
        D = np.diag(d)
        L = D - W
        # Dn = D^(-1/2)
        Dn = np.power(np.linalg.matrix_power(D, -1), 0.5)
        Lbar = np.dot(np.dot(Dn, L), Dn)
        return Lbar

    def getKSmallestEigVec(self, Lbar, k):
        # """input
        # "matrix Lbar and k
        # "return
        # "k smallest eigen values and their corresponding eigen vectors
        # """
        eigval,eigvec = linalg.eig(Lbar)
        dim = len(eigval)

        # 查找前k小的eigval
        dictEigval = dict(zip(eigval, range(0, dim)))
        kEig = np.sort(eigval)[0:k]
        ix = [dictEigval[i] for i in kEig]
        return eigval[ix],eigvec[:,ix]

    def checkResult(self, Lbar, eigvec, eigval, k):
        # """
        # "input
        # "matrix Lbar and k eig values and k eig vectors
        # "print norm(Lbar*eigvec[:,i]-lamda[i]*eigvec[:,i])
        # """
        check = [np.dot(Lbar,eigvec[:,i]) - eigval[i]*eigvec[:,i] \
          for i in xrange(0,k)]
        length = [np.linalg.norm(e) for e in check]/np.spacing(1)
        print("Lbar*v-lamda*v are %s*%s" % (length,np.spacing(1)))
    ################################################################################

    def sc_exec(self, logs, K):
        # ofname = "query_moyan"
        # K = 2
        # yes | no
        # verbose = sys.argv[4]
        verbose = "no"

        query = "-"
        url_uid_date_dict = {}
        site_url_dict = {}
        query_id,uid_id,date_id,url_id,title_id,refer_id = \
          0,3,4,5,6,7

        for i in xrange(len(logs)):
            # nquery uid date nurl title refer_url
            words = logs[i]
            # print "\t".join(words)
            if len(words) != 6:
                continue
            url = words[url_id]
            # 0117ae6bb23b480db7ffa206bac294e1	1384257478	20131112	黑魔烟	\
            #   http://wenda.so.com/q/1365225225067381	黑魔香烟是什么_360问答	\
            #   http://so.360.cn/s?ie=utf-8&src=hao_search&q=%E9%BB%91%E9%AD%94%E7%83%9F
            site = words[url_id].strip("http://").split("/")[0].strip("www.").split(".")[0].strip()
            #site = url.strip("http://").split("/")[0].strip("www.")
            if site not in site_url_dict:
                site_url_dict[site] = set()
            # site_url_dict[site].add("\t".join([words[query_id], words[url_id], words[title_id]]))
            site_url_dict[site].add(words[url_id])
            
            #### delete baike
            if site not in ("baike"):
                if site not in url_uid_date_dict:
                    url_uid_date_dict[site] = {}
                if words[uid_id] not in url_uid_date_dict[site]:
                    url_uid_date_dict[site][words[uid_id]] = set()
                url_uid_date_dict[site][words[uid_id]].add(words[date_id])
            query = words[query_id]
        ############################################################################

        url_id = {}
        id_url = {}
        cnt = 0
        for url in url_uid_date_dict:
            if url not in url_id:
                url_id[url] = cnt
                id_url[cnt] = url
                cnt += 1
        if "baike" not in url_id:
            url_id["baike"] = cnt
            id_url[cnt] = "baike"
        if verbose == "yes":
            print "DEBUG: len = %d"%(len(url_id)), "\turl_id\n", url_id
            print "DEBUG: len = %d"%(len(id_url)), "\tid_url\n", id_url

        g = nx.Graph()
        # fop = open("com_" + ofname, "w")
        for uL in url_uid_date_dict:
            for uR in url_uid_date_dict:
                if uL != uR:
                    setLU = set(url_uid_date_dict[uL].keys())
                    setRU = set(url_uid_date_dict[uR].keys())
                    setCU = setLU & setRU
                    com_sum_1d = 0
                    for u in setCU:
                        setLDate = url_uid_date_dict[uL][u] 
                        setRDate = url_uid_date_dict[uR][u]
                        setCDate = setLDate & setRDate
                        com_sum_1d += len(setCDate)
                    if com_sum_1d > 0:
                        if verbose == "yes":
                            print "\t".join(["com_1d:%d"%(com_sum_1d), \
                              "uL:%s"%(uL), "uR:%s"%(uR)])
                            # fop.write("\t".join(["com_1d:%d"%(com_sum_1d), \
                              #  "uL:%s"%(uL), "uR:%s"%(uR)]) + "\n")
                        g.add_edge(url_id[uL], url_id[uR], \
                          weight=1.0 * com_sum_1d)
        # fop.close()
        ################################################################################
        # print "DEBUG: \ng.nodes:", g.nodes()
        # print "len of nodes = %d" %(len(g.nodes()))
        # nodeNum=len(g.nodes())
        m=nx.to_numpy_matrix(g)
        if verbose == "yes":
            print "matrix: ", m

        Lbar = self.getNormLaplacian(m)
        # K=3
        kEigVal,kEigVec = self.getKSmallestEigVec(Lbar, K)
        if verbose == "yes":
            print("k eig val are %s" % kEigVal)
            print("k eig vec are %s" % kEigVec)
            self.checkResult(Lbar, kEigVec, kEigVal, K)

        center = kmeans(kEigVec, K, iter=1000)[0]
        result = vq(kEigVec, center)[0]
        if verbose == "yes":
            print "len of res instances =%d"%(len(result))
            print result

        gnodelist = g.nodes()
        dicClus = {}
        for i in xrange(len(result)):
            if dicClus.has_key(result[i]):
                dicClus[result[i]].append(id_url[gnodelist[i]])
            else:
                dicClus[result[i]] = [id_url[gnodelist[i]]]

        # fwp = open(ofname + "_Cluster_Res" + "-K%d"%(K) + ".txt", "w")
        for k in dicClus:
            for site in dicClus[k]:
                for url in site_url_dict[site]:
                    # pstr = query + "\tCluster-%d\t"%(k) + "id-%d\t"%(url_id[site]) + url
                    pstr = query + "\tCID-%d\t"%(k) + url
                    print pstr
                    #fwp.write(pstr + "\n")
        #### baike ####
        if "baike" in site_url_dict:
            for url in site_url_dict["baike"]:
                # pstr = query + "\tCluster-%d\t"%(K) + "id-%d\t"%(url_id["baike"]) + url
                pstr = query + "\tCID-%d\t"%(K) + url
                print pstr
                # fwp.write(pstr + "\n")
        # fwp.close()
        # drawPic(g, result, K)
        del m
        del g
        del url_uid_date_dict
        del site_url_dict
        del gnodelist
        del dicClus
        del Lbar
        del kEigVec
        del center
        del result
        del url_id
        del id_url
        del kEigVal

