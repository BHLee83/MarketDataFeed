import matplotlib.pyplot as mpl
import scipy.cluster.hierarchy as sch, random,numpy as np, pandas as pd

def getIVP(cov, **kargs):
    #compute the inverse-variance portfolio
    ivp = 1/np.diag(cov)
    ivp /= ivp.sum()
    return ivp

def getClusterVar(cov, cItems):
    #compute variance per cluster
    cov_ = cov.loc[cItems, cItems]
    w_ = getIVP(cov_).reshape(-1,1)
    cVar = np.dot(np.dot(w_.T, cov_), w_)[0,0]
    return cVar

def getQuasiDiag(link):
    #sort clustered items by distance
    link = link.astype(int)
    sortIx = pd.Series([link[-1,0], link[-1,1]])
    numItems = link[-1,3] #number of original items
    while sortIx.max() >= numItems:
        sortIx.index = range(0, sortIx.shape[0]*2,2)
        df0 = sortIx[sortIx>=numItems] # find clusters
        i = df0.index; j=df0.values-numItems
        sortIx[i] = link[j,0]
        df0 = pd.Series(link[j,1], index = i+1)
        # sortIx = sortIx.append(df0)
        sortIx = pd.concat([sortIx, df0])
        sortIx = sortIx.sort_index()
        sortIx.index = range(sortIx.shape[0]) #re-index
    return sortIx.tolist()

def getRecBipart(cov, sortIx):
    #compute HRP alloc
    w = pd.Series(1, index = sortIx)
    cItems = [sortIx] #initialize all items in one cluster
    while len(cItems)>0:
        print(cItems)
        cItems = [i[j:k] for i in cItems for j,k in ((0,int(len(i)/2)), (int(len(i)/2), len(i))) if len(i) > 1]
        for i in range(0, len(cItems),2):
            cItems0 = cItems[i]
            cItems1 = cItems[i+1]
            cVar0 = getClusterVar(cov, cItems0)
            cVar1 = getClusterVar(cov, cItems1)
            alpha = 1 - (cVar0/(cVar0 + cVar1))
            w[cItems0] *= alpha
            w[cItems1] *= 1- alpha
    return w

def correlDist(corr):
    #A distance matrix based on correlation matrix, where 0<=d[i,j]<=1
    dist = ((1-corr)/2)**0.5
    return dist

def generateData(nObs, size0, size1, sigma1):
    #Time Series of correlated variables
    # 1. generating some uncorrelated data
    np.random.seed(seed = 12345); random.seed(12345)
    x = np.random.normal(0,1,size = (nObs, size0)) # each row is a variable
    
    # 2. creating correlation between the variables
    cols = [random.randint(0, size0-1) for i in range(size1)]
    y = x[:, cols] + np.random.normal(0,sigma1, size = (nObs, len(cols)))
    x = np.append(x,y,axis = 1)
    x = pd.DataFrame(x, columns = range(1,x.shape[1]+1))
    return x, cols

def main():
    nObs, size0, size1, sigma1 = 10000,5,5,0.25
    x, cols = generateData(nObs, size0, size1, sigma1)
    print([(j+1, size0+1) for i,j in enumerate(cols,1)])
    
    cov, corr = x.cov(), x.corr()
    
    dist = correlDist(corr)
    link = sch.linkage(dist, 'single')
    sortIx = getQuasiDiag(link)
    sortIx = corr.index[sortIx].tolist()
    
    hrp = getRecBipart(cov, sortIx)
    print(hrp)
    return