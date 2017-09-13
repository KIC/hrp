sortIx=['a2', 'a4', 'a1', 'a3', 'a5']
cItems=[sortIx]
println(cItems)

while (cItems.size()>0) {
    // cItems=[i[j:k] for i in cItems for j,k in ((0,len(i)/2), (len(i)/2,len(i))) if len(i)>1]
    cItems = cItems.findAll { it -> it.size() > 1 }
                   .collectMany { it -> return [it.subList(0, it.size().intdiv(2)), it.subList(it.size().intdiv(2), it.size())] }

    println(cItems)
}
