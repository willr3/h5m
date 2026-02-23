# performance testing 

This is a simple upload test using 100 runs from a production use case.


## Steps

1. Create a `states.yaml` to set the following values for your local environment
```yaml
states:
  asprof_dir: 
  h5m_folder: 
  H5M_PATH:
```

2. Run the test using qDup

```shell

java -jar qDup.jar -S gzip_secret="secret_goes_here" -b /tmp/ states.yaml quarkus-spring-boot-comparison.yam
```

### Collecting data

The test will automatically capture the time it takes to compile h5m and upload the 100 runs.
```shell
jq '.state.upload.real' run.json
```
and validates the number of uploaded files and calculated values
```shell
jq '.state | "\(.uploads) \(.values)"' run.json
```

We can additionall collect a jfr or html flamegraph by setting `profiler` to either `jfr` or `async`

```shell
java -jar qDup.jar -S profiler="asprof" -S gzip_secret="secret_goes_here" -b /tmp/ states.yaml quarkus-spring-boot-comparison.yam
```

Make sure `asprof_dir` is set in the `states.yaml`