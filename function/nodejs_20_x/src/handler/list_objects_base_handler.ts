/*
eslint-disable @typescript-eslint/strict-boolean-expressions,
*/
import { BaseObjectContext, UserRequest } from '../s3objectlambda_event.types';
import { Buffer } from 'buffer';
import { IBaseListObject } from '../s3objectlambda_list_type';
import { IErrorResponse, IListObjectsResponse, IResponse } from '../s3objectlambda_response.types';
import { ListObjectsXmlTransformer } from '../utils/listobject_xml_transformer';
import { ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';
import { makeS3RequestOriginal } from '../request/utils';
import { errorResponse, responseForS3Errors } from '../error/error_response';
import ErrorCode from '../error/error_code';

/**
 * Class that handles ListObjects requests. Can be used
 * for both ListObjectsV1 and ListObjectsV2 requests
 */
export class ListObjectsHandler <T extends IBaseListObject> {
  private readonly transformObject: (listObject: IBaseListObject) => IBaseListObject;
  private readonly XMLTransformer = new ListObjectsXmlTransformer<T>();

  constructor (transformObject: (listObject: IBaseListObject) => IBaseListObject) {
    this.transformObject = transformObject;
  }

  /**
   * Handles a ListObjects request, by performing the following steps:
   * 1. Validates the incoming user request.
   * 2. Retrieves the original object from Amazon S3. Converts it into an Object.
   * 3. Applies a transformation. You can apply your custom transformation logic here.
   * 4. Sends the final transformed object back to Amazon S3 Object Lambda.
   */
  async handleListObjectsRequest ({ s3Client, cloudflare }: {s3Client: S3Client, cloudflare: S3Client}, requestContext: BaseObjectContext, userRequest: UserRequest):
  Promise<IResponse> {
    const url = new URL(decodeURIComponent(requestContext.inputS3Url));

    if (url.searchParams.get('prefix')?.startsWith('verify_')) {
      return this.handleListObjectsRequestOriginal(requestContext, userRequest);
    }
    const originalResponse = await makeS3RequestOriginal(requestContext.inputS3Url, userRequest, 'GET');
    console.log('Original response from S3:', ListObjectsHandler.stringFromArrayBuffer(await originalResponse.arrayBuffer()));
    try {
      console.log('Prefix:', requestContext.inputS3Url, requestContext);
      const response = await cloudflare.send(new ListObjectsV2Command({
        Bucket: 'andy-redirect-bj66n98jif7a6z6fqd41csrbuse1a--ol-s3',
        Prefix: url.searchParams.get('prefix') ?? '',
        Delimiter: url.searchParams.get('delimiter') ?? ''
      }));
      console.log('Response from cloudflare:', response);
      const objectResponse: IBaseListObject = {
        ...response,

        IsTruncated: response.IsTruncated ? response.IsTruncated : false,
        ...(response.EncodingType ? { EncodingType: response.EncodingType } : {}),
        MaxKeys: response.MaxKeys ?? 0,
        ...(response.Prefix ? { Prefix: response.Prefix } : {}),
        Contents: response.Contents?.map(x => ({
          ...x,
          Key: x.Key ?? '',
          LastModified: x.LastModified?.toISOString() ?? ''
        }))?.filter(x => !(x.Size === 0 && x.Key.endsWith('/'))) as any,
        ...(response.Delimiter ? { Delimiter: response.Delimiter } : {}),
        CommonPrefixes: response.CommonPrefixes ?? [] as any
      };

      // @ts-expect-error
      delete objectResponse.$metadata;

      const transformedObject = this.transformObject(objectResponse);

      return this.writeResponse(transformedObject);
    } catch (e) {
      console.error('Error occurred in list:', e);
      const out: IErrorResponse = {
        statusCode: 500,
        errorMessage: (e as Error).message
      };
      return out;
    }
  }

  async handleListObjectsRequestOriginal (requestContext: BaseObjectContext, userRequest: UserRequest):
  Promise<IResponse> {
    const objectResponse = await makeS3RequestOriginal(requestContext.inputS3Url, userRequest, 'GET');

    const originalObject = await objectResponse.arrayBuffer();

    if (objectResponse.status >= 400) {
      // Errors in the Amazon S3 response should be forwarded to the caller without invoking transformObject.
      return responseForS3Errors(objectResponse);
    }

    const parsedObject = await this.XMLTransformer.createListObjectsJsonResponse(ListObjectsHandler.stringFromArrayBuffer(originalObject));

    if (parsedObject == null) {
      console.log('Failure parsing the response from S3');
      return errorResponse(requestContext, ErrorCode.NO_SUCH_KEY, 'Requested key does not exist');
    }

    const transformedObject = this.transformObject(parsedObject);

    return this.writeResponse(transformedObject);
  }

  /**
   * Returns the response expected by Object Lambda on a LIST_OBJECTS request
   * @param objectResponse The response
   * @protected
   */
  protected writeResponse (objectResponse: IBaseListObject): IListObjectsResponse | IErrorResponse {
    console.log('Sending transformed results to the Object Lambda Access Point', objectResponse);
    const xmlListObject = this.XMLTransformer.createListObjectsXmlResponse(objectResponse);

    if (xmlListObject === null) {
      console.log('Failed transforming back to XML');
      return {
        statusCode: 500,
        errorMessage: 'The Lambda function failed to transform the result to XML'
      };
    }

    console.log('Successfully transformed back to XML', xmlListObject);
    return {
      statusCode: 200,
      listResultXml: xmlListObject
    };
  }

  /**
   * Converts from the array buffer received to a string object.
   * @param arrayBuffer The array buffer containing the string
   * @private
   */
  private static stringFromArrayBuffer (arrayBuffer: ArrayBuffer): string {
    return Buffer.from(arrayBuffer).toString();
  }
}
