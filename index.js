const KinesisClass = require("aws-sdk/clients/kinesis");
const objectSize = require("object-sizeof");

const Kinesis = new KinesisClass({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.secretAccessKey,
  region: process.env.AWS_DEFAULT_REGION,
});

let maxSizeForStreaming
let EVENT_STREAM_NAME
let STREAM_PUT_TIMEOUT

const initialise = (streamName, sizePerRequest, requestInterval) => {
    maxSizeForStreaming = parseInt(sizePerRequest) * 1000000;
    EVENT_STREAM_NAME = streamName;
    STREAM_PUT_TIMEOUT = requestInterval;
}

async function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const isStreamFeasible = (payloads) =>
  objectSize(JSON.stringify(payloads)) < maxSizeForStreaming;

const matchKinesisPutConditions = (payloads) => {
  if (!isStreamFeasible(payloads)) {
    const tempBuffer = [];
    while (!isStreamFeasible(payloads)) {
      tempBuffer.push(payloads.pop());
    }
    return [payloads, ...matchKinesisPutConditions(tempBuffer)];
  }
  return [payloads];
};

const pushToKinesis = (kinesisPayloads) =>
  Kinesis.putRecords({
    Records: kinesisPayloads,
    StreamName: EVENT_STREAM_NAME,
  }).promise();

const retryFailedRecords = async (retryRecordSet) => {
  await sleep(STREAM_PUT_TIMEOUT);
  console.log("retrying failed record: ", retryRecordSet.length);
  const res = await pushToKinesis(retryRecordSet);
  const tranformedRecordsInRetry =
    res.Records.length - (res.FailedRecordCount ? res.FailedRecordCount : 0);
  if (res.FailedRecordCount) {
    const failedRecordsLength = res.Records.filter((rec) => !!rec.ErrorCode)
      .length;
    const failedRecords = retryRecordSet.splice(-1 * failedRecordsLength);
    return retryFailedRecords(failedRecords) + tranformedRecordsInRetry;
  }
  return tranformedRecordsInRetry;
};


/**
 * @param {Array.<Object>} parsedRecords array of Records to put on Kinesis
 * @typedef {Object} Kinesis_Params
 * @property {String} streamName name of the Kinesis Stream
 * @property {String} partitionKeyName name of the field to be used as partiton key. (must be unique)
 * @param {Kinesis_Params} kinesis_params
 * @typedef {Object} Options
 * @property {number} requestInterval delay between each consecutive requests in ms, defaults 880ms
 * @property {number} sizePerRequest size of payload per request in MiB from (0 to 5), defaults 4.5 MiB 
 * @param {Options} options
 */
const putRecordToKinesisStream = async (parsedRecords, kinesis_params = {streamName, partitionKeyName}, options = { requestInterval = 880, sizePerRequest = 4.5 }) => {
    try {
    initialise(kinesis_params.streamName, options.sizePerRequest, options.requestInterval)
    const kinesisPayloads = [];
    const pushRecordsToKinesisPayloads = (record) => {
      const params = {
        Data: JSON.stringify(record),
        PartitionKey: record[kinesis_params.partitionKeyName],
      };
      kinesisPayloads.push(params);
    };
    parsedRecords.forEach(pushRecordsToKinesisPayloads);
    const buffer = matchKinesisPutConditions(kinesisPayloads);
    for (const recordSet of buffer) {
      const res = await pushToKinesis(recordSet);
      if (res.FailedRecordCount) {
        const failedRecordsLength = res.Records.filter((rec) => !!rec.ErrorCode)
          .length;
        const failedRecords = recordSet.splice(-1 * failedRecordsLength);
        await retryFailedRecords(
          recordSet.splice(failedRecords)
        );
      }
      await sleep(STREAM_PUT_TIMEOUT);
    }
  } catch (error) {
    console.error("PutRecordsToStream: ERROR in pushing record to kinesis stream", error);
    throw error;
  }
};

module.exports = putRecordToKinesisStream