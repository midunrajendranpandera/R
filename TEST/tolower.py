__author__ = 'Pandera'

#zindex_distribution1={["Name:'Experience',Score:22"]["Name:'Job Fit',Score:23"]}
#zindex_distribution={["Name:'Experience',Score:22"]}
zd1={}
zd1["Name"]='Experience'
zd1["Score"]=22
zd2={}
zd2["Name"]='Job Fit'
zd2["Score"]=22
zd=[]
zd.append(zd1)
zd.append(zd2)
zd=dict(zd)
print(type(zd))
#zd((k.lower(), v) for k,v in {'My Key':'My Value'}.iteritems())

print(zd)