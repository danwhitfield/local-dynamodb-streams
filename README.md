# local-dynamodb-streams

Emulates the streams functionality of DynamoDB for local development

## Usage

### Docker Build

```bash
docker build -t local-dynamodb-streams:1.0 . 
```

### Docker Compose

```yaml
  local-dynamodb-streams:
    image: local-dynamodb-streams:1.0
    environment:
      - AWS_REGION=ap-southeast-2
      - TABLE_NAME=<NAME OF DYNAMODB TABLE>
      - SAM_TEMPLATE_FILE=/app/cdk.out/<NAME OF CLOUDFORMATION TEMPLATE>.json
      - ENV_FILE=/app/resources/env.json
      - LAMBDA_FUNCTION_NAME=<NAME OF FUNCTION TO INVOKE>
      - CDK_OUT_ABSOLUTE_PATH=${PWD}/cdk.out
      - SAM_CLI_TELEMETRY=0
      - AWS_ENDPOINT=http://localstack:4566
      - DOCKER_NETWORK=<YOUR DOCKER NETWORK NAME>
    volumes:
      - '/var/run/docker.sock:/var/run/docker.sock'
      - './cdk.out:/app/cdk.out'
      - './resources:/app/resources'
    depends_on:
      - localstack
    networks:
      - default
```
