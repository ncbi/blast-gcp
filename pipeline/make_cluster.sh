#!/usr/bin/env bash
set -o nounset
set -o pipefail

usage="
usage: $0
Required environment variables:
    project
    region
    zone
    deploy_id
    deploy_user
    deploy_gcs_url
    blast_dataproc_cluster_name
    blast_dataproc_cluster_max_age
"

[ $# -eq 0 ] || { echo ${usage} && exit 1; }

checkvar=${project?"${usage}"}
checkvar=${region?"${usage}"}
checkvar=${zone?"${usage}"}
checkvar=${deploy_id?"${usage}"}
checkvar=${deploy_user?"${usage}"}
checkvar=${blast_dataproc_cluster_name?"${usage}"}
checkvar=${blast_dataproc_cluster_max_age?"${usage}"}

gcloud dataproc clusters describe ${blast_dataproc_cluster_name} --region ${region} > /dev/null
if [[ $? -eq 0 ]]
then
    printf "Reusing dataproc cluster [%s]\n" ${blast_dataproc_cluster_name}
else
    printf "Creating dataproc cluster [%s]\n" ${blast_dataproc_cluster_name}
    gcloud beta dataproc clusters create ${blast_dataproc_cluster_name} \
        --master-machine-type n1-standard-4 --master-boot-disk-size 100 \
        --num-workers 2 --worker-boot-disk-size 171 --worker-machine-type custom-64-151552  \
        --num-preemptible-workers 0 --preemptible-worker-boot-disk-size 171 --scopes cloud-platform \
        --project ${project} \
        --labels owner=${deploy_user},deploy_id=${deploy_id} \
        --region ${region} \
        --zone ${zone} \
        --max-age=${blast_dataproc_cluster_max_age} \
        --tags ${blast_dataproc_cluster_name} \
        --image-version 1.2 \
        --initialization-action-timeout 30m \
        --initialization-actions gs://blastgcp-pipeline-test/scripts/cluster_initialize.sh \
        --bucket dataproc-3bd9289a-e273-42db-9248-bd33fb5aee33-us-east4
fi


address_url=${deploy_gcs_url}/address.txt
gcloud compute instances describe ${blast_dataproc_cluster_name}-m --zone ${zone} |
awk '/ *networkIP:/ { print $2; printf "writing master node IP address [%s]\n", $2 > "/dev/stderr" }' |
gsutil cp - ${address_url}

