# InfluxDB ELT

## Useful commands

Find the total number of channels currently in the destination bucket.

```shell
let total_count=0;
for name in $(s3cmd ls 's3://prod-lakehouse-source-accelerator-influxdb/machinestate/' | grep -v _dlt | grep -v '/init' | awk '{print $2}' | cut -d '/' -f 5 | xargs); do
    _count=$(s3cmd ls s3://prod-lakehouse-source-accelerator-influxdb/machinestate/$name/ | wc -l);
    echo $name $((_count));
    total_count=$(($total_count+$_count));
done;
echo;
echo "Total channels read $total_count"
```
