# rwdqueryservice
A service to query a custom built inverted bitmap (hybrid) index.

## Buliding
### In Xcode/Mac
Open Package.swift and build.

### On Linux/Windows
- For release
`swift build -c release --product "App" --static-swift-stdlib -Xlinker -ljemalloc`

- For debug
`swift build --product "App" -Xlinker -ljemalloc`

### In a docker
git pull
sudo docker build -t rwdqueryservice .


## Running
### In Xcode/Mac

Navigate to Product/Scheme/Edit Scheme/Arguments tab. Set the following environment variables.
LOG_LEVEL=debug
INDEX_FILE_PATH=<path to .rwdx index file>
OPENAI_API_KEY=<optionally provide API key>
MULTUM_MAP_FILE_PATH=<path to multum_mapping.csv file>
OCI_CONFIG_DIR=<OCI config directory, usually ~/.oci, this is used for local development>
OCI_PROFILE=<OCI profile name>
DEPLOYMENT_MODE=<openai for using direct API key, genaiSessionAuth for using GenAI service with session token auth, genaiInstanceAuth for using GenAI service with Instance Principal auth>
OCI_COMPARTMENT_OCID=<compartment OCID for GenAI>

### On Linux
(export INDEX_FILE_PATH='<path to .rwdx index file>'; export MULTUM_MAP_FILE_PATH='<path to multum_mapping.csv file>'; export DEPLOYMENT_MODE='cloud'; export OCI_COMPARTMENT_OCID='<comparment OCID>'; .build/x86_64-unknown-linux-gnu/release/App --hostname 0.0.0.0 --port 8000)

### In a docker
- Set environment variables in docker-compose.yaml
sudo docker-compose -p app down app
sudo docker-compose -f ./docker-compose.yml -p app up -d app 
