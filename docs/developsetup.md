# Setup the project from Source Code

You can build and run the project from source code following the steps below


## Install .NET 5 

Follow the install instructions from the link below:

https://docs.microsoft.com/en-us/dotnet/core/install/linux-ubuntu#2104-

## Instal Python 3.8

https://www.python.org/downloads/release/python-387/

# Request Generator

Go to the Generator folder
```
cd Generator/
```

## Building Generator
Setup the generator with following command:
```
./setup.sh
```

## Running the Request Generator
To see all the custom run options type the ``rundev.sh`` script with --help arg
```
./run.sh --help
```
Example: running generator with default values for target and method args, but with a custom entries amount.
```
./run.sh --entries 50
```

ps: execute the scripts above from the ``Generator`` directory

### Result

The output will be in the file "requests.parquet" inside ``results`` folder

# Request Sender
Go to the Sender folder
```
cd Sender/
```

## Building Sender
Setup the sender with following command:
```
./setup.sh
```

## Running the Request Sender
To see all the custom run options type the ``rundev.sh`` script with --help arg
```
./run.sh --help
```

Example: running request sender
```
./run.sh --host 127.0.0.1:8000 --user user0_cert.pem --pk user0_privk.pem --cacert networkcert.pem
```

ps: execute the scripts above from the ``Sender`` directory

### Result

The output will be in the files "responses.parquet" and "sentrequests.parquet" inside ``results`` folder

# Analyzer

Go to the Analyzer folder
```
cd Analyzer
```

## Installing Analyzer requirements
Install the analyzer lib requirements with the setup script:
```
./setup.sh
```

## Running Analyzer
To see all the custom run options type the ``run.sh`` script with --help arg
```
./run.sh --help
```
Example: running the analyzer script
```
./run.sh --responsesfile /../results/responses.parquet
```

ps: execute the scripts above from the ``Analyzer`` directory

### Result

The output will be in command line

ps: the input files with default params need to be inside ``results`` folder

