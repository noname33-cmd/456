#!/bin/bash
NODES="node_97 node_148 node_150 node_151 node_152 node_120 node_121"
for n in $NODES; do
  ssh $n "/minjust-74-250/gbdfl/pattern_controller/scripts/run_agent_node.sh $n"
done
