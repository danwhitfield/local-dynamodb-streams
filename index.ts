import {
  DescribeStreamCommand,
  DynamoDBStreamsClient,
  ExpiredIteratorException,
  GetRecordsCommand,
  GetShardIteratorCommand,
  ListStreamsCommand,
  _Record
} from '@aws-sdk/client-dynamodb-streams'
import { exec } from 'child_process'
import * as fs from 'fs/promises'
import path from 'path'

const TABLE_NAME = process.env.TABLE_NAME
const SAM_TEMPLATE_FILE = process.env.SAM_TEMPLATE_FILE
const LAMBDA_FUNCTION_NAME = process.env.LAMBDA_FUNCTION_NAME
const ENV_FILE = process.env.ENV_FILE
const CDK_OUT_ABSOLUTE_PATH = process.env.CDK_OUT_ABSOLUTE_PATH
const AWS_ENDPOINT = process.env.AWS_ENDPOINT
const TMP_EVENT_FILE_NAME = path.resolve(__dirname, '.dynamodb-stream-event.json')
const AWS_REGION = process.env.AWS_REGION
const DOCKER_NETWORK = process.env.DOCKER_NETWORK

const client = new DynamoDBStreamsClient({
  endpoint: AWS_ENDPOINT,
  region: AWS_REGION
})

async function execShellCommand(command: string): Promise<any> {
  return new Promise((resolve, reject) => {
    console.log(`Running shell command: ${command}`)

    exec(command, (err, stdout, stderr) => {
      if (err) {
        console.error(err)
        console.error(stderr)
        reject(stderr)
      }

      resolve(stdout)
    })
  })
}

async function getRecords(shardIterator: string): Promise<[records: Array<_Record>, nextShardIterator: string]> {
  const response = await client.send(
    new GetRecordsCommand({
      ShardIterator: shardIterator
    })
  )

  if (!response.NextShardIterator) {
    throw new Error(`No next shard iterator returned for shard iterator '${shardIterator}'!`)
  }

  if (typeof response.Records === 'undefined') {
    throw new Error(`No defined records returned for shard iterator '${shardIterator}'!`)
  }

  return [response.Records, response.NextShardIterator]
}

async function getInitialShardIterator(streamArn: string, shardId: string): Promise<string> {
  const response = await client.send(
    new GetShardIteratorCommand({
      ShardId: shardId,
      StreamArn: streamArn,
      ShardIteratorType: 'LATEST'
    })
  )

  if (!response.ShardIterator) {
    throw new Error(`Unable to get shard iterator for stream ARN '${streamArn}' and shard ID '${shardId}'!`)
  }

  return response.ShardIterator
}

async function getShardId(streamArn: string): Promise<string | undefined> {
  const response = await client.send(
    new DescribeStreamCommand({
      StreamArn: streamArn
    })
  )

  if (!response.StreamDescription?.Shards) {
    console.warn(`Failed to find shards for stream ARN '${streamArn}'!`)
    return
  }

  if (response.StreamDescription.Shards.length > 1) {
    console.warn(`Multiple shards found for stream ARN '${streamArn}'`)
    return
  }

  if (!response.StreamDescription.Shards[0].ShardId) {
    console.warn(`No shard ID found on shard for stream ARN '${streamArn}'!`)
    return
  }

  return response.StreamDescription.Shards[0].ShardId
}

async function getStreamArn(): Promise<string | undefined> {
  const response = await client.send(
    new ListStreamsCommand({
      TableName: TABLE_NAME
    })
  )

  if (!response.Streams) {
    console.warn(`No DynamoDB Streams found for table '${TABLE_NAME}'!`)
    return
  }

  if (response.Streams.length > 1) {
    console.warn(`Found multiple streams on table '${TABLE_NAME}'!`)
    return
  }

  if (!response.Streams[0]?.StreamArn) {
    console.warn(`No ARN on stream for table '${TABLE_NAME}'!`)
    return
  }

  return response.Streams[0].StreamArn
}

async function sleep(seconds: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, seconds * 1000)
  })
}

async function invokeLambda(records: _Record[]): Promise<void> {
  const event = { Records: records }

  console.log(`Writing event to '${TMP_EVENT_FILE_NAME}'`)

  try {
    await fs.unlink(TMP_EVENT_FILE_NAME)
  } catch (err) {}

  await fs.writeFile(TMP_EVENT_FILE_NAME, JSON.stringify(event))

  return await execShellCommand(
    `sam local invoke --docker-network ${DOCKER_NETWORK} --docker-volume-basedir ${CDK_OUT_ABSOLUTE_PATH} --container-host host.docker.internal --container-host-interface 0.0.0.0 --region ${AWS_REGION} --env-vars ${ENV_FILE} --template ${SAM_TEMPLATE_FILE} --event ${TMP_EVENT_FILE_NAME} ${LAMBDA_FUNCTION_NAME} > /proc/1/fd/1 2>/proc/1/fd/2 3>&1 4>&2`
  )
}

async function main() {
  if (!TABLE_NAME) {
    throw new Error('You must configure the TABLE_NAME environment variable!')
  }

  let streamArn
  let shardId

  const sleepSeconds = 5

  do {
    streamArn = await getStreamArn()

    if (streamArn) {
      shardId = await getShardId(streamArn)
    }

    await sleep(sleepSeconds)
  } while (!streamArn && !shardId)

  if (!streamArn || !shardId) {
    throw new Error(`Failed to determine stream ARN and shard ID for DynamoDB table '${TABLE_NAME}'!`)
  }

  let shardIterator = await getInitialShardIterator(streamArn, shardId)

  while (true) {
    console.log('Polling for new DynamoDB stream records...')

    try {
      const [records, nextShardIterator] = await getRecords(shardIterator)

      shardIterator = nextShardIterator

      if (records.length) {
        console.log(`Found ${records.length} records, invoking Lambda...`)

        await invokeLambda(records)
      }
    } catch (err) {
      if (err instanceof ExpiredIteratorException) {
        console.log('Iterator expired, creating new initial shard iterator...')
        shardIterator = await getInitialShardIterator(streamArn, shardId)
      } else {
        throw err
      }
    }

    console.log(`Sleeping for ${sleepSeconds} seconds...`)

    await sleep(sleepSeconds)
  }
}

main()
