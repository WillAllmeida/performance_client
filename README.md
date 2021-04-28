# Performance_client

Go to the step1 MVP folder
```
cd mvp_step1_client/
```

## Install .NET 5 

Follow the install instructions from the link below:

https://docs.microsoft.com/en-us/dotnet/core/install/linux-ubuntu#2010-

## Building project
Setup the project with following command:
```
./setup.sh
```

## Running the Request Generator
To see all the custom run options type the ``run.sh`` script with --help arg
```
./run.sh --help
```
Example running generator with default values for target and method args, but with a custom entries amount.
```
./run.sh --entries 50
```

ps: execute the scripts above from the ``mvp_step1_client`` directory

### Result

The output will be in the file "requests.parquet" inside ``results`` folder

## Running the Request Sender
Example running request sender
```
./run.sh --host 127.0.0.1:8000 --user user0_cert.pem --pk user0_privk.pem --cacert networkcert.pem
```

ps: execute the scripts above from the ``Request Sender`` directory

### Result

The output will be in the files "responserequests.parquet" and "sentrequests.parquet" inside ``results`` folder
