const KinesisClass = require('aws-sdk/clients/kinesis');
const objectSize = require('object-sizeof');
const assert = require('assert');

const Kinesis = new KinesisClass({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.secretAccessKey,
  region: process.env.AWS_DEFAULT_REGION,
});

let maxSizeForStreaming;
let EVENT_STREAM_NAME;
let STREAM_PUT_TIMEOUT;

const initialise = (streamName, sizePerRequest, requestInterval) => {
  maxSizeForStreaming = parseInt(sizePerRequest) * 1000000;
  EVENT_STREAM_NAME = streamName;
  STREAM_PUT_TIMEOUT = requestInterval;
};

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const isStreamFeasible = (payloads) => objectSize(JSON.stringify(payloads)) < maxSizeForStreaming && payloads.length <= 500;

const chunkKinesisPayload = (payloads) => {
  if (!isStreamFeasible(payloads)) {
    const tempBuffer = [];
    while (!isStreamFeasible(payloads)) {
      tempBuffer.push(payloads.pop());
    }
    return [payloads, ...chunkKinesisPayload(tempBuffer)];
  }
  return [payloads];
};

const pushToKinesis = async (kinesisPayloads) =>
  Kinesis.putRecords({
    Records: kinesisPayloads,
    StreamName: EVENT_STREAM_NAME,
  }).promise();

/**
 * @param  {Array.<Object>} retryRecordSet array of Records to put on Kinesis
 * This function  will be called for the records failed to be pushed in the first attempt
 * due to threshold limit of the shards getting exceeded.
 */
const retryFailedRecords = async (retryRecordSet) => {
  // delaying retry, so that all records may pass in single attempt
  await sleep(STREAM_PUT_TIMEOUT);
  console.log('retrying failed record: ', retryRecordSet.length);
  const res = await pushToKinesis(retryRecordSet);
  const tranformedRecordsInRetry = res.Records.length - (res.FailedRecordCount ? res.FailedRecordCount : 0);
  if (res.FailedRecordCount) {
    const failedRecordsLength = res.Records.filter((rec) => !!rec.ErrorCode).length;
    const failedRecords = retryRecordSet.splice(-1 * failedRecordsLength);
    return retryFailedRecords(failedRecords) + tranformedRecordsInRetry;
  }
  return tranformedRecordsInRetry;
};

/**
 * @param {Array.<Object>} payload array of Records to put on Kinesis
 * @param {Object} config
 * @param {String} config.streamName name of the Kinesis Stream
 * @param {String} config.partitionKeyName name of the field to be used as partiton key from the payload object. Must be unique.
 * @param {number} [config.requestInterval=880] - delay between each consecutive requests in ms. Defaults 880ms
 * @param {number} [config.sizePerRequest=4.5] - sizePerRequest size of payload per request in MiB from (0 to 5). Defaults 4.5 MiB.
 */
const putRecordToKinesisStream = async (payload, config) => {
  assert(config.streamName, 'Provde a valid stream name for Kinesis');
  assert(config.partitionKeyName, 'Provde a valid partitionKey field-name for kinesis records');
  try {
    initialise(config.streamName, config.sizePerRequest || 4.5, config.requestInterval || 880);
    const kinesisPayloads = [];
    const pushRecordsToKinesisPayloads = (record) => {
      assert(record[config.partitionKeyName], `Missing field used as partition key in payload object ${JSON.stringify(record, null, 2)}`)
      const params = {
        Data: JSON.stringify(record),
        PartitionKey: record[config.partitionKeyName],
      };
      kinesisPayloads.push(params);
    };
    payload.forEach(pushRecordsToKinesisPayloads);
    const chunkedKinesisPayloads = chunkKinesisPayload(kinesisPayloads);
    for (const currentKinesisPayloads of chunkedKinesisPayloads) {
      const res = await pushToKinesis(currentKinesisPayloads);
      if (res.FailedRecordCount) {
        const failedRecordsLength = res.Records.filter((rec) => !!rec.ErrorCode).length;
        const failedRecords = currentKinesisPayloads.splice(-1 * failedRecordsLength);
        // slicing the failed record to retry
        await retryFailedRecords(failedRecords);
      }
      // delay between each chunk
      await sleep(STREAM_PUT_TIMEOUT);
    }
  } catch (error) {
    console.error('PutRecordsToStream: ERROR in pushing record to kinesis stream', error);
    throw error;
  }
};

module.exports = putRecordToKinesisStream;
