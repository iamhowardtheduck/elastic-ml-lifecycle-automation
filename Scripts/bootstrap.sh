pip install elasticsearch faker
python bootstrap-classification.py \
    --host http://kubernetes-vm:30920 \
    --kibana-host http://kubernetes-vm:30002 \
    --user sdg \
    --password changeme \
    --no-verify-ssl
