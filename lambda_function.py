import json
import boto3
import platform
from time import sleep
from concurrent.futures import ThreadPoolExecutor

s3c = boto3.client("s3")
s3r = boto3.resource('s3')

def print_log(*arg: tuple):
    '''ログ出力
    '''
    print(*arg)

def upload_part_copy(src_bucket, src_key, target_bucket, target_key, part_num, copy_source_range, upload_id, parts_etags):
    '''マルチパートでS3オブジェクトの一部（指定範囲）をコピーする
    '''
    try:
        response = s3c.upload_part_copy(
            Bucket=target_bucket,
            CopySource={'Bucket': src_bucket, 'Key': src_key},
            CopySourceRange=copy_source_range,
            Key=target_key,
            PartNumber=part_num,
            UploadId=upload_id
        )
        parts_etags.append({"ETag": response["CopyPartResult"]["ETag"], "PartNumber": part_num})
    except Exception as e:
        print_log(f"Error while CREATING UPLOAD_PART_COPY")
        raise e

def copy_with_multipart(src_bucket, src_key, target_bucket, target_key, object_size, metadata):
    '''マルチパートでS3オブジェクトをコピーする
    '''
    try:
        initiate_multipart = s3c.create_multipart_upload(
            Bucket=target_bucket,
            Key=target_key,
            Metadata=metadata
        )
        upload_id = initiate_multipart['UploadId']
        #print_log("upload_id: ", upload_id)
        part_size = 64 * 1024 * 1024 # part size[byte]
        #print_log("part_size: ", part_size)
        byte_position = 0

        parts_etags = []

        max_range = -(-object_size // part_size) # 切り上げ
        #print_log("max_range: ", max_range)

        with ThreadPoolExecutor(max_workers=10) as executor: # s3.ap-northeast-3.amazonaws.com. Connection pool size: 10
            for part_num in range(1, max_range + 1):
                last_byte = min(byte_position + part_size - 1, object_size - 1)
                copy_source_range = f"bytes={byte_position}-{last_byte}"
                executor.submit(upload_part_copy, src_bucket, src_key, target_bucket, target_key, part_num, copy_source_range, upload_id, parts_etags)
                byte_position += part_size
                sleep(1)
        try:
            parts_etags = sorted(parts_etags, key=lambda x:x['PartNumber'])
            response = s3c.complete_multipart_upload(
                Bucket=target_bucket,
                Key=target_key,
                MultipartUpload={
                    'Parts': parts_etags
                },
                UploadId=upload_id
            )
            print_log(f"COMPLETE_MULTIPART_UPLOAD COMPLETED SUCCESSFULLY, response={response}")
        except Exception as e:
            print_log(f"Error while CREATING COMPLETE_MULTIPART_UPLOAD")
            raise e
    except Exception as e:
        print_log(f"Error while CREATING CREATE_MULTIPART_UPLOAD")
        raise e


def lambda_handler(event, context):

    print_log('start')

    # 動作環境確認
    print_log("python_version : ", platform.python_version())
    print_log("boto3.version : ", boto3.__version__)

    # S3バケット名
    src_bucket_name = 'sun-ada-bucket-tokyo'
    tgt_bucket_name = 'sun-ada-bucket-osaka'

    # S3オブジェクト名
    cpy_object_name = 'bigfile_001mb'
    #cpy_object_name = 'bigfile_010mb'
    #cpy_object_name = 'bigfile_100mb'
    #cpy_object_name = 'bigfile_500mb'
    #cpy_object_name = 'bigfile_01gb'
    #cpy_object_name = 'bigfile_02gb'
    #cpy_object_name = 'bigfile_05gb'
    #cpy_object_name = 'bigfile_08gb'
    #cpy_object_name = 'bigfile_10gb'
    #cpy_object_name = 'bigfile_20gb'

    copy_source = {
        'Bucket': src_bucket_name,
        'Key': cpy_object_name
    }

    print_log(cpy_object_name)
    
    try:
        # コピー元オブジェクト情報
        print_log(src_bucket_name)
        src_res_head_object = s3c.head_object(Bucket=src_bucket_name, Key=cpy_object_name)
        src_metadata = src_res_head_object["Metadata"]
        print_log("Metadata : ", src_metadata)

        src_res_get_object_tagging = s3c.get_object_tagging(Bucket=src_bucket_name, Key=cpy_object_name)
        src_tagset = src_res_get_object_tagging["TagSet"]
        print_log("TagSet : ", src_tagset)

        contentLength = src_res_head_object["ContentLength"]
        print_log("ContentLength : : ", contentLength)

        # ユーザー定義のメタデータ追加付与
        src_metadata["add"] = "metadata"

        # オブジェクトコピー
        #s3c.copy_object(CopySource=copy_source, Bucket=tgt_bucket_name, Key=cpy_object_name)
        #s3r.meta.client.copy(copy_source, tgt_bucket_name, cpy_object_name)
        copy_with_multipart(
            src_bucket_name,
            cpy_object_name,
            tgt_bucket_name,
            cpy_object_name,
            contentLength,
            src_metadata
        )
        # タグ情報もコピー先へ設定
        s3c.put_object_tagging(Bucket=tgt_bucket_name, Key=cpy_object_name, Tagging={'TagSet': src_tagset})

        # コピー先オブジェクト情報
        print_log(tgt_bucket_name)
        tgt_res_head_object = s3c.head_object(Bucket=tgt_bucket_name, Key=cpy_object_name)
        tgt_metadata = tgt_res_head_object["Metadata"]
        print_log("Metadata : ", tgt_metadata)

        tgt_res_get_object_tagging = s3c.get_object_tagging(Bucket=tgt_bucket_name, Key=cpy_object_name)
        tgt_tagset = src_res_get_object_tagging["TagSet"]
        print_log("TagSet : ", tgt_tagset)

        print_log('success')
        
    except Exception as e:
        print_log('error: ', e)

    finally:
        print_log('finally')
        return {
            'statusCode': 200,
            'body': json.dumps('Hello from Lambda!')
        }
