/**
 * fonty Telemetry ETL
 * @author James Ooi <wengteikooi@gmail.com>
 * @license Apache License 2.0
 */
import axios from 'axios';
import geoip from 'geoip-lite';
import PubSub from '@google-cloud/pubsub';
import BigQuery from '@google-cloud/bigquery';
const iso3166 = require('../data/iso3166-1.json');


/**
 * Configuration
 */
const options: PubSub.GCloudConfiguration = {
  projectId: 'fonty-analytics',
  keyFilename: 'config/credentials.json'
}


/**
 * An interface representing a Google PubSub message.
 */
interface PubSubMessage {
  id: string
  ackId: string
  data: Buffer
  attributes: any
  timestamp: string
  ack(): void
  nack(): void
}


/**
 * An interface representing an incoming telemetry event.
 */
interface IncomingTelemetryEvent {
  ip_address: string
  server_timestamp: number
  timestamp: string
  status_code: number
  event_type: string
  execution_time: number
  fonty_version: string
  os_family: string
  os_version: string
  python_version: string
  data: any
}


/**
 * An interface representing the processed telemetry event data.
 */
interface ProcessedTelemetryEvent {
  server_timestamp: number
  timestamp: string
  geo_country_code: string
  geo_country: string
  geo_region: string
  geo_coords: string
  status_code: number
  event_type: string
  execution_time: number
  fonty_version: string
  os_family: string
  os_version: string
  python_version: string
  event_data?: Array<EventData>
}


/**
 * An interface representing a single event data.
 */
interface EventData {
  key: string
  value: string
}


/**
 * Entry function.
 */
function main() {
  const pubsub = PubSub(options);
  const bigquery = BigQuery(options);
  const subscription = pubsub.subscription('fonty-telemetry-etl');
  subscription.on('message', (message) => processMessage(message, bigquery));
}


/**
 * Function to process incoming messages.
 */
async function processMessage(message: PubSubMessage, bigquery: any) {
  const data: IncomingTelemetryEvent = JSON.parse(message.data.toString('utf8'));

  // Lookup geographical location
  const geo = geoip.lookup(data.ip_address);

  // Resolve font source name
  if (data.data !== null && 'source_url' in data.data) {
    data.data['source_name'] = await resolveFontSource(data.data.source_url);
  }

  // Flatten event data into an array of keys and values
  let eventData: Array<EventData> = [];
  if (data.data !== null) {
    eventData = Object.keys(data.data).map(key => ({
      key: key,
      value: data.data[key] === null ? null : String(data.data[key])
    }));
  }

  // Craft processed data
  const processed: ProcessedTelemetryEvent = {
    server_timestamp: data.server_timestamp,
    timestamp: data.timestamp,
    geo_country_code: geo.country,
    geo_country: iso3166[geo.country].name || null,
    geo_region: geo.region,
    geo_coords: `${geo.ll[0]}, ${geo.ll[1]}`,
    status_code: data.status_code,
    event_type: data.event_type,
    execution_time: data.execution_time,
    fonty_version: data.fonty_version,
    os_family: data.os_family,
    os_version: data.os_version,
    python_version: data.python_version,
    event_data: eventData,
  }

  // Store data into BigQuery
  try {
    const insert = await bigquery
      .dataset('fonty_telemetry')
      .table('events')
      .insert([processed]);
  } catch(err) {
    console.log(err.errors[0].error);
  }

  // Acknowledge the message
  message.ack()
}


/**
 * Function to resolve font source names.
 */
async function resolveFontSource(sourceUrl: string): Promise<string> {
  if (sourceUrl in _sourceCache) {
    return _sourceCache[sourceUrl];
  }
  try {
    const res = await axios.get(sourceUrl);
    _sourceCache[sourceUrl] = res.data.name;
    return res.data.name;
  } catch {
    return null;
  }
}

const _sourceCache: { [key: string]: string } = {};


export default main;