#!/bin/bash
cat > /minjust-74-250/gbdfl/pattern_controller/signals/haproxy_ops/rq_test_disable.json <<EOF
{"op":"disable","scope":"runtime","backend":"Jboss_client","server":"$1","note":"test"}
EOF
