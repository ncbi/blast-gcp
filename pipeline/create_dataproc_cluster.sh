#!/usr/bin/env bash
set -o nounset
set -o pipefail

usage="
usage: $0 <inifile>
Required variables from environment and/or ini:
    project
    region
    zone
    deploy_id
    deploy_user
    deploy_gcs_url
    blast_dataproc_cluster_name
    blast_dataproc_cluster_max_age
"

[ $# -eq 1 ] || { echo ${usage} && exit 1; }

INIFILE=$1

set -e
source ${INIFILE}

checkvar=${project?"${usage}"}
checkvar=${region?"${usage}"}
checkvar=${zone?"${usage}"}
checkvar=${labels?"${usage}"}
checkvar=${deploy_id?"${usage}"}
checkvar=${deploy_user?"${usage}"}
checkvar=${blast_dataproc_cluster_name?"${usage}"}
checkvar=${master_machine_type?"${usage}"}
checkvar=${master_boot_disk_size?"${usage}"}
checkvar=${num_workers?"${usage}"}
checkvar=${worker_machine_type?"${usage}"}
checkvar=${worker_boot_disk_size?"${usage}"}
checkvar=${num_preemptible_workers?"${usage}"}
checkvar=${blast_dataproc_cluster_max_age?"${usage}"}
checkvar=${image_version?"${usage}"}

set +e
gcloud dataproc clusters describe ${blast_dataproc_cluster_name} --region ${region} > /dev/null
if [[ $? -eq 0 ]]
then
    printf "Reusing dataproc cluster [%s]\n" ${blast_dataproc_cluster_name}
else
    printf "Creating dataproc cluster [%s]\n" ${blast_dataproc_cluster_name}
    gcloud beta dataproc clusters create ${blast_dataproc_cluster_name} \
        --master-machine-type ${master_machine_type} \
        --master-boot-disk-size ${master_boot_disk_size} \
        --num-workers ${num_workers} \
        --worker-boot-disk-size ${worker_boot_disk_size} \
        --worker-machine-type ${worker_machine_type} \
        --num-preemptible-workers ${num_preemptible_workers} \
        --preemptible-worker-boot-disk-size ${worker_boot_disk_size} \
        --scopes cloud-platform \
        --project ${project} \
        --labels ${labels} \
        --region ${region} \
        --zone ${zone} \
        --max-age=${blast_dataproc_cluster_max_age} \
        --tags ${blast_dataproc_cluster_name} \
        --image-version ${image_version} \
        --initialization-action-timeout 30m \
        --initialization-actions gs://blastgcp-pipeline-test/scripts/cluster_initialize.sh \
        --bucket dataproc-3bd9289a-e273-42db-9248-bd33fb5aee33-us-east4
fi


address_url=${deploy_gcs_url}/address.txt
gcloud compute instances describe ${blast_dataproc_cluster_name}-m --zone ${zone} |
awk '/ *networkIP:/ { print $2; printf "writing master node IP address [%s]\n", $2 > "/dev/stderr" }' |
gsutil cp - ${address_url}

