from graphviz import Digraph
import csv
import pandas as pd
import sys

### Read in file inputs/outputs for processes.
dfi = pd.read_csv('./vint_ins.csv')
dfo = pd.read_csv('./vint_outs.csv')


### Full process connection graph
dfi2 = dfi.drop_duplicates(['process_id', 'input_process_id'])
dot = Digraph(comment="Process Flow")
dot.attr('Node',shape='box')

for idx, edge in dfi2.iterrows():
    #print edge['process_id'], edge['input_process_id']
    dot.edge(edge['input_process_id'],edge['process_id'])

print(dot.source)
dot.render('./all_processflow.gv',view=True)





### Process file connection graph, specific process
focusprocess='edf'
dfi2 = dfi.drop_duplicates(['process_id', 'input_table'])
dfo2 = dfo.drop_duplicates(['process_id', 'table'])
dfi3 = dfi2.loc[dfi2['process_id'] == focusprocess]

specdot = Digraph(comment="Process Flow",engine='fdp')
specdot.attr('Node',shape='box')

for process in pd.unique(dfi3['input_process_id']):
    infiles = dfi3.loc[dfi3['input_process_id'] == process]
    with specdot.subgraph(name="".join(["cluster_",process])) as s:
        s.attr(label=process)
        for fidx,inf in infiles.iterrows():
            s.node(inf['input_table'])
    specdot.edge("".join(["cluster_",process]),focusprocess)
    #specdot.edge(process,focusprocess)

specdot.view()


#### NOTE: Doesn't do outputs yet!

sys.exit("Stopping early")

for idx, edge in dfi2.iterrows():
    #print edge['process_id'], edge['input_process_id']
    if edge['input_process_id'] == process or edge['process_id'] == process:
        specdot.edge(edge['input_process_id'],edge['process_id'],label=edge['instgen'])

    print(specdot.source)
    specdot.render("".join(['./',process,'_processflow.gv']),view=True)
    specdot.clear()



### Process connection graph, specific process
dfi2 = dfi.drop_duplicates(['process_id', 'input_process_id','instgen'])
specdot = Digraph(comment="Process Flow")
specdot.attr('Node',shape='box')

for process in pd.unique(dfi['process_id']):
    for idx, edge in dfi2.iterrows():
        #print edge['process_id'], edge['input_process_id']
        if edge['input_process_id'] == process or edge['process_id'] == process:
            specdot.edge(edge['input_process_id'],edge['process_id'],label=edge['instgen'])

    print(specdot.source)
    specdot.render("".join(['./',process,'_processflow.gv']),view=True)
    specdot.clear()

