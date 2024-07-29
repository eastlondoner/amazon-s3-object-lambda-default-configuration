/**
 * Contains utility methods for Request handling, such as extracting query parameters.
 */
import { GetObjectCommand, HeadObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { mapPartNumber, mapPartNumberHead } from '../response/part_number_mapper';
import { mapRange, mapRangeHead } from '../response/range_mapper';
import { RangeResponse } from '../response/range_response.types';
import { UserRequest } from '../s3objectlambda_event.types';
import fetch, { Response } from 'node-fetch';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';

// Query parameters names
const RANGE = 'Range';
const PART_NUMBER = 'partNumber';

// Header constants
export const CONTENT_LENGTH = 'content-length';

/**
 * Get the part number from the user request
 * @param userRequest The user request
 */
export function getPartNumber (userRequest: UserRequest): string | null {
  // PartNumber can be present as a request query parameter.
  return getQueryParam(userRequest.url, PART_NUMBER);
}

/**
 * Get the range from the user request
 * @param userRequest The user request
 */
export function getRange (userRequest: UserRequest): string | null {
  // Convert object to a TypeScript Map
  const headersMap = new Map(Object.entries(userRequest.headers).map(([k, v]) => [k.toLowerCase(), v]));

  // Range can be present as a request header or query parameter.
  if (headersMap.has(RANGE.toLowerCase())) {
    return headersMap.get(RANGE.toLowerCase());
  } else {
    return getQueryParam(userRequest.url, RANGE);
  }
}

/**
 * Check if the request context has range or partNumber parameter. This helps us handle a ranged request
 * and return only the requested range to the GetObject caller. For more information on range and partNumber,
 * see {@link https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax|GetObject Request Syntax}
 * in the Amazon S3 API Reference.
 * @param transformedObject Object on which the Range or Part number is going to be applied to
 * @param userRequest The user request where it's checking if range or part number is specified
 * @returns The object having range or part number applied on them if they have been specfied in the user request
 */
export function applyRangeOrPartNumber (transformedObject: Buffer, userRequest: UserRequest): RangeResponse {
  const range = getRange(userRequest);
  const partNumber = getPartNumber(userRequest);

  if (range != null) {
    return mapRange(range, transformedObject);
  }

  if (partNumber != null) {
    return mapPartNumber(partNumber, transformedObject);
  }

  // The request was made for the whole object, so return as is.
  return { object: transformedObject, hasError: false };
}

/**
 * Check if the request context has range or partNumber parameter. This helps us handle a ranged request
 * and return only the requested range to the HeadObject caller. For more information on range and partNumber,
 * see {@link https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadObject.html#API_HeadObject_RequestSyntax|HeadObject
  * Request Syntax}
 * in the Amazon S3 API Reference.
 * @param transformedHeaders Map of the
 * @param userRequest The user request where it's checking if range or part number is specified
 * @returns The object having range or part number applied on them if they have been specfied in the user request
 */
export function applyRangeOrPartNumberHeaders (transformedHeaders: Map<string, object>,
  userRequest: UserRequest): RangeResponse {
  const range = getRange(userRequest);
  const partNumber = getPartNumber(userRequest);

  if (range != null) {
    return mapRangeHead(range, transformedHeaders);
  }

  if (partNumber != null) {
    return mapPartNumberHead(partNumber, transformedHeaders);
  }

  // The request was made for the whole object, so return as is.
  return { headers: transformedHeaders, hasError: false };
}

/**
 * Gets a query parameter from the url, converted to lower case. Returns null, in case it doesn't exist in the url.
 * @param url The url from where the query parameter is going to be extracted
 * @param name The name of the specific query parameter.
 */
function getQueryParam (url: string, name: string): string | null {
  url = url.toLowerCase();
  name = name.toLowerCase();
  return new URL(url).searchParams.get(name);
}

export async function makeS3Request (cloudflare: S3Client, url: string, userRequest: UserRequest, method: 'GET' | 'HEAD'): Promise<Response> {
  const key = new URL(decodeURIComponent(url)).pathname.slice(1);
  // const originalResult = await makeS3RequestOriginal(url, userRequest, method);
  // if (key.startsWith('verify_')) {
  //   return originalResult;
  // }
  const headers = removeSignedRequestHeaders(userRequest.headers);
  console.log(`Got request for ${url}`, key, headers);
  url = await getSignedUrl(cloudflare,
    method === 'GET'
      ? new GetObjectCommand({
        Bucket: 'andy-redirect-bj66n98jif7a6z6fqd41csrbuse1a--ol-s3',
        Key: key,
        // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
        ...(headers.range ? { Range: headers.range ?? headers.Range } : {}),
        // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
        ...(headers.partnumber ? { PartNumber: parseInt(headers.partnumber ?? headers.PartNumber ?? headers.partNumber) } : {})
      })
      : new HeadObjectCommand({
        Bucket: 'andy-redirect-bj66n98jif7a6z6fqd41csrbuse1a--ol-s3',
        Key: key
      }), { expiresIn: 300 });

  console.log(`Making request to ${url}`, headers);
  const response = await fetch(url, {
    method,
    headers: {
      // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
      ...(headers.range ? { range: headers.range } : {}),
      // eslint-disable-next-line @typescript-eslint/strict-boolean-expressions
      ...(headers.partnumber ? { partnumber: headers.partnumber } : {})
      // Host: 'b9576518138804e154b4e8e36806fce5.r2.cloudflarestorage.com'
    }
  });
  console.log(`Got response from ${url.split('?')[0]}`, response.status);
  return response;
}
/**
 * Get all headers that should be included in the pre-signed S3 URL. We do not add headers that will be
 * applied after transformation, such as Range.
 */
export function getRequestHeaders (headersObj: object): Map<string, string> {
  const headersMap: Map<string, string> = new Map();
  const headersToBePresigned = ['x-amz-checksum-mode', 'x-amz-request-payer', 'x-amz-expected-bucket-owner', 'If-Match',
    'If-Modified-Since', 'If-None-Match', 'If-Unmodified-Since'];

  new Map(Object.entries(headersObj)).forEach((value: string, key: string) => {
    if (headersToBePresigned.includes(key)) {
      headersMap.set(key, value);
    }
  });

  return headersMap;
}

/**
 * Get all headers that should be included in the pre-signed S3 URL. We do not add headers that will be
 * applied after transformation, such as Range.
 */
export function removeSignedRequestHeaders (headersObj: object): Record<string, string> {
  const headersMap: Record<string, string> = {};
  const headersToBePresigned = ['Host', 'host', 'x-amz-checksum-mode', 'x-amz-request-payer', 'x-amz-expected-bucket-owner', 'If-Match',
    'If-Modified-Since', 'If-None-Match', 'If-Unmodified-Since'];

  new Map(Object.entries(headersObj)).forEach((value: string, key: string) => {
    if (!headersToBePresigned.includes(key)) {
      headersMap[key] = value;
    }
  });

  return headersMap;
}

export async function makeS3RequestOriginal (url: string, userRequest: UserRequest, method: 'GET' | 'HEAD'): Promise<Response> {
  const requestHeaders = getRequestHeaders(userRequest.headers);
  // TODO: handle fetch errors
  return fetch(url, {
    method,
    headers: Object.fromEntries(requestHeaders)
  });
}
