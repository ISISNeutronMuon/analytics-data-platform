#!/bin/bash
#
# Script to perform some initial checks on recommended memory, CPUs and disk space for running Airflow in Docker.
# Also creates missing directories in /opt/airflow and sets ownership to the user running the container.
#

set -e

if [[ -z "${AIRFLOW_UID}" ]]; then
  echo
  echo -e "\033[1;33mWARNING!!!: AIRFLOW_UID not set!\e[0m"
  echo "If you are on Linux, you SHOULD set AIRFLOW_UID."
  echo
  AIRFLOW_UID=$(id -u)
  export AIRFLOW_UID
fi

one_meg=1048576
mem_available=$(( $(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / one_meg ))
cpus_available=$(grep -cE 'cpu[0-9]+' /proc/stat)
disk_available=$(df / | tail -1 | awk '{print $4}')

warning_resources="false"

if (( mem_available < 4000 )); then
  echo
  echo -e "\033[1;33mWARNING!!!: Not enough memory available for Docker.\e[0m"
  echo "At least 4GB required."
  warning_resources="true"
fi

if (( cpus_available < 2 )); then
  echo
  echo -e "\033[1;33mWARNING!!!: Not enough CPUs available for Docker.\e[0m"
  warning_resources="true"
fi

if (( disk_available < one_meg * 10 )); then
  echo
  echo -e "\033[1;33mWARNING!!!: Not enough Disk space available for Docker.\e[0m"
  warning_resources="true"
fi

if [[ "${warning_resources}" == "true" ]]; then
  echo
  echo -e "\033[1;33mWARNING!!!: You may not have enough resources to run Airflow.\e[0m"
fi

echo
echo "Creating airflow directories in /opt/airflow if they do not exist."
mkdir -p /opt/airflow/{logs,dags,plugins,config,uv_cache}

echo
echo "Airflow version:"
/entrypoint airflow version

echo
echo "Change ownership of files in logs to ${AIRFLOW_UID:-50000}:0"
chown -R "${AIRFLOW_UID:-50000}:0" /opt/airflow/{logs,uv_cache}

echo
echo "Files in shared volumes:"
ls -la /opt/airflow/{logs,dags,plugins,config}
