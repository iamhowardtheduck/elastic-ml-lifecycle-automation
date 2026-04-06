pip install elasticsearch faker
python /workspace/workshop/elastic-ml-lifecycle-automation/bootstrap-classification.py \
    --host http://kubernetes-vm:30920 \
    --kibana-host http://kubernetes-vm:30002 \
    --user sdg \
    --password changeme \
    --no-verify-ssl
